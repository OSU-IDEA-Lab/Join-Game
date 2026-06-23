# ROSL — Alternative Version<br>Fixed Number of Deterministic Exploration Probes<br>Epsilon Greedy on Observed Joinability in Exploitation

A sampling-based join operator for PostgreSQL that replaces the inner loop of
the executor's nested-loop node with a joinability-guided (bandit-style) sampler, and
produces a **running estimate of the total join size** as it streams matching
rows. ROSL trades exactness for early, progressively-accurate estimates: it can
report an approximate `|R ⋈ S|` long before a full join would finish.

This document describes both the algorithm (per the design notes) and the
PostgreSQL implementation in `nodeNestloop.c`, including how the estimator
communicates results and how the benchmark harness consumes them.

---

## 1. What ROSL does

Given outer relation `R` and inner relation `S`, ROSL operates in two phases
per M-block:

1. Consumes `R` in **M-blocks** of `m_lim` tuples. On the first M-block, all
   of `S` is **materialised once** into a shared buffer. The first `n_probes`
   tuples form the fixed **exploration prefix P**; the remainder is the
   **exploitation body B = S \ P** (`|B| = |S| − n_probes`).
2. **Exploration (region R × P):** every tuple `t` in the M-block is probed
   against all `n_probes` prefix tuples. Because `a(t) = n_probes` is the same
   known constant for every tuple, the empirical join rate `q(t) = r(t) /
   n_probes` is unambiguous with no untried-tuple edge case. These pairs are
   observed with probability 1 — each match is emitted and tallied exactly into
   `expl_exact`; no inverse-probability weighting is applied.
3. **Exploitation (region R × B):** with `q(t)` frozen from exploration and
   `ε` fixed as a hyperparameter, an exploit cache is drawn from the
   joinability-weighted distribution once per K-block and probed against each
   body K-block in turn, emitting matches as output rows.
4. A per-round **Horvitz–Thompson** estimate of the body region is folded into
   a running total; combined with `expl_exact` it gives `Ĵ`, the two-region
   estimated join size.

The bandit logic concentrates probing on tuples with high empirical joinability
(exploitation) while retaining a floor of random exploration so every tuple
keeps a non-zero chance of being sampled.

---

## 2. The estimator (Horvitz–Thompson)

ROSL's estimate is unbiased-by-design through inverse-probability weighting on
the body region only; the exploration region is counted exactly.

### Per-round estimate (body only)

For exploitation round `(i, j)` — M-block `Mᵢ` against body K-block `Kⱼ` —
with deduplicated exploit cache `Cᵢⱼ`:

```
Ŷ_ij = Σ_{t ∈ Cᵢⱼ}  ( Σ_{s ∈ Kⱼ} 1[t ⋈ s] )  /  π_ij(t)
```

where `π_ij(t)` is the **inclusion probability** of tuple `t` — the probability
that `t` is drawn at least once across the `L = exp_cache_lim` with-replacement
draws:

```
π_ij(t) = 1 − (1 − p_ij(t))^L
```

and `p_ij(t)` is `t`'s single-draw selection probability under that round's
distribution. Dividing each tuple's observed match count by `π` removes the
cache-selection bias and rescales the cache's contribution up to the full
M-block.

The inclusion probability is used **as written** — it is *not* renormalized
across cache tuples. The single-draw distribution `p` is normalized to sum to 1
over the M-block before sampling; `π` then follows directly from the binomial
"drawn at least once" probability.

### Two-region running estimate

`R × S` is partitioned into two disjoint regions. The prefix region `R × P` is
observed in full (M-blocks tile `R`; each tuple is probed against all of `P`),
so its matches are counted exactly. Only the body region `R × B` requires
statistical estimation. The running total is:

```
μ̂_B = ( Σ_{(i,j) ∈ P} Ŷ_ij ) / ( Σ_{(i,j) ∈ P} |Mᵢ|·|Kⱼ| )
Ĵ   = expl_exact  +  μ̂_B · |R| · |B|
```

`μ̂_B` is the estimated mean join successes per body tuple-pair over all body
rounds processed so far; `|B| = |S| − n_probes` is the body size. Pairing the
body-only denominator with the body-only multiplier `|B|` (rather than `|S|`)
keeps the estimate unbiased; scaling the body mean by the full `|S|` would
double-count the prefix. Removing `R × P` from the estimated population also
reduces variance, since those `n_probes · |R|` prefix pairs contribute exact
outcomes and no estimator noise.

### Sampling distribution (custom ε-smoothing)

Because every tuple in the M-block is probed exactly `n_probes` times during
exploration, `a(t) = n_probes` is a known positive constant and the average
joinability reduces to:

```
q(t) = r(t) / n_probes
```

There is no untried-tuple edge case. With `Q_total = Σ_{t ∈ A} q(t)`:

```
p(t) = (1 − ε)·q(t)/Q_total + ε/|A|     if Q_total > 0
p(t) = 1/|A|                             if Q_total = 0
```

`Q_total = 0` arises when no tuple joined during exploration; the distribution
falls back to uniform since there is no joinability signal. `q(t)` and `ε` are
both **fixed for the entire M-block** — `q(t)` is frozen when exploration ends
and `ε` is a hyperparameter, not a decaying schedule — so every exploitation
round within one M-block draws from the identical distribution.

### Fixed epsilon

`ε` is a single fixed hyperparameter (`ROSL_EPSILON`) shared across all rounds
and all M-blocks. There is no decay schedule. Keeping `ε > 0` guarantees every
`p(t) ≥ ε/|A|`, bounding the inverse-probability weights and preventing
estimator spikes from near-zero-probability tuples.

**Known limitations of the current sampler** (from the design notes):

- The distribution is rebuilt every K-block round rather than stored once. In
  the fixed-probe design `q(t)` and `ε` are both frozen, so each rebuild
  produces the identical distribution; the cost is O(|M|) per round and small
  relative to probing, but the recomputation itself is redundant.

---

## 3. Implementation (`nodeNestloop.c`)

ROSL is implemented inside PostgreSQL's nested-loop executor node. The stock
node and ROSL share the same `NestLoopState`; ROSL's working state is allocated
lazily the first time it runs.

### Behaviour is gated by a GUC

```
SET enable_rosl = off;   -- exact stock nested loop (default)
SET enable_rosl = on;    -- ROSL sampling join + running estimate
```

With `enable_rosl = off`, the node behaves exactly like the unmodified nested
loop — there is no overhead and no behavioural change. Registering the boolean
GUC requires the usual entry in `guc.c`.

### Tunable constants

Defined at the top of the file:

| Constant              | Value     | Meaning                                                      |
|-----------------------|-----------|--------------------------------------------------------------|
| `ROSL_M_LIM`          | 1588      | Outer (R) tuples per M-block                                  |
| `ROSL_K_LIM`          | 1588      | Inner (S) body tuples per K-block                             |
| `ROSL_EXP_CACHE_LIM`  | 529       | With-replacement draws per exploitation round (`L`)           |
| `ROSL_N_PROBES`       | 100       | Shared exploration prefix size; `a(t)` for every M-tuple      |
| `ROSL_EPSILON`        | 0.2       | Fixed exploration mass (`ε` hyperparameter)                   |
| `ROSL_TRAJ_CAP`       | 1,000,000 | Max per-round trajectory rows retained for the dump           |

### State machine

A single `ExecNestLoop` call advances a small phase machine and yields at most
one row per call (resume cursors let each phase span many calls):

- **`PH_NEW_MBLOCK`** — load the next outer block; if `R` is exhausted, go to
  `PH_DONE`. On the first M-block, materialise all of `S` into the shared
  buffer (reused unchanged by every subsequent M-block); zero `reward[]` for
  the new block; transition to `PH_EXPLORE`.
- **`PH_EXPLORE`** — probe every M-tuple against the shared prefix
  `s_slots[0 .. n_probes)`, incrementing `r(t)` on each join and accumulating
  exact matches into `expl_exact`. Resume cursors `expl_mi` / `expl_pi` let
  the phase yield one row per call. When all M-tuples are done, `q(t)` is
  frozen and the body scan begins at `k_start = n_probes`.
- **`PH_NEW_SBLOCK`** — take the next contiguous K-block from the body
  (`s_slots[k_start .. k_start + k_count)`); build the distribution from
  frozen `q(t)` and fixed `ε`; draw and deduplicate the exploit cache; reset
  per-round match counts.
- **`PH_PROBE`** — probe the exploit cache against the body K-block, emitting
  matches one per call. When the round is complete, fold the Horvitz–Thompson
  body estimate (`finalize_round`) and advance `k_start` to the next K-block.
  `ε` is fixed; there is no decay step.
- **`PH_DONE`** — outer relation exhausted; the cursor returns NULL.

Completion is signalled to the client purely by the cursor returning NULL — no
mid-stream message is emitted. (See §4.)

---

## 4. Output protocol — server-log only

ROSL does **not** stream estimates back to the client mid-query. Instead it
accumulates the entire per-round trajectory in its executor state and dumps it
**once, at executor teardown** (`ExecEndNestLoop` → `PrintRoslCounters`), as
`elog(INFO, ...)` lines to the **PostgreSQL server log**. Row delivery and
measurement delivery are on separate channels.

This decoupling is deliberate: an earlier design emitted a per-round `NOTICE`
during row production, which deadlocked a server-side cursor because notices are
only flushed at fetch boundaries. With the log-only dump, nothing the client
must parse is interleaved with rows, so that class of hang is structurally
impossible.

### Log line formats

Per-round trajectory (one line per exploitation round):

```
ROSL_TRAJ round=<n> mean_per_pair=<μ̂_B> est_join=<Ĵ> pairs_seen=<den> sample_matches=<m> elapsed_ms=<t>
```

`mean_per_pair` records `μ̂_B` — the HT mean over body pairs only. `est_join`
is the two-region total `expl_exact + μ̂_B · |R| · |B|`.

Final summary (one line at teardown):

```
ROSL_SUMM final_est_join=<Ĵ> rounds=<n> sample_matches=<m> pairs_seen=<den> ...
```

If a run exceeds `ROSL_TRAJ_CAP` rounds, the first `ROSL_TRAJ_CAP` are kept and
a `ROSL_TRAJ truncated: ...` line is emitted (use a smaller scale factor).

Per-round `elapsed_ms` is measured in C at the source (relative to the first
M-block load), independent of when the client fetches rows.

### Cluster requirements

For the dump to reach the log and be readable:

```
logging_collector = on        # a managed logfile exists on disk (restart-only GUC)
log_min_messages  = info       # INFO actually reaches the log (worker sets per session)
log_line_prefix   includes %p  # per-backend PID, so concurrent workers can be told apart
```

On long-running jobs the collector may rotate the logfile mid-run (default
`log_rotation_size = 10MB`); for the cleanest behaviour during a large sweep,
pin a single file with `log_rotation_size = 0` and `log_rotation_age = 0`, then
`SELECT pg_reload_conf();`.

---

## 5. Running it

### Configure the plan

ROSL must run as a plain, fixed-inner nested loop. The benchmark worker sets
(among others):

```
SET enable_rosl = on;
SET enable_nestloop = on;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_indexscan = off;       -- inner must not be a parameterized index scan
SET enable_material = off;        -- no Materialize between NL and inner
SET log_min_messages = info;      -- estimator dump reaches the server log
```

### Benchmark harness

The accompanying Python harness drives sweeps over (size, skew, shuffle, query):

- **`tpch_manager.py`** — fans out one worker subprocess per configuration,
  caps concurrency, times each job, and merges per-job CSVs into
  `trajectory.csv` / `summary.csv`.
- **`worker.py`** — for one configuration: computes the ground-truth join size
  once (forced hash join, cached in `truth_counts.json`), runs ROSL, drains its
  rows to force teardown, then reads this run's `ROSL_TRAJ`/`ROSL_SUMM` lines
  from the server log (isolated by backend PID + a unique per-run sentinel) and
  writes the per-round trajectory and per-run summary.
- **`accuracy_charts.py`** — reads the merged `trajectory.csv` and plots
  estimator error (`|rel_error|`) versus fraction of true output sampled, one
  line per shuffle plus an averaged line, per (query, size, skew).

Output limits are applied per query and must stay in sync across the manager,
worker, and chart script:

| Query | LIMIT       |
|-------|-------------|
| Q9    | 221,700     |
| Q10   | 13,000      |
| Q11   | 327,624,700 |
| Q12   | 1,000       |
| Q15   | 43,800      |

### Trajectory CSV columns

```
size, query, zval, shuffle, repeat, round, pairs_seen, sample_matches,
mean_per_pair, est_join, truth, ratio, rel_error, elapsed_sec, delta_sec,
pct_of_truth_output
```

`ratio = est_join / truth` and `rel_error = (est_join − truth)/truth` are the
accuracy columns; `pct_of_truth_output` (fraction of the true join output
sampled so far) is the natural x-axis for accuracy-vs-progress charts.
`mean_per_pair` is `μ̂_B` (body mean); `est_join` is the two-region total.

---

## 6. File map

| File                  | Role                                                            |
|-----------------------|-----------------------------------------------------------------|
| `nodeNestloop.c`      | ROSL executor node (sampling join + HT estimator + log dump)    |
| `tpch_manager.py`     | Sweep manager: fan-out, timing, CSV merge                       |
| `worker.py`           | Per-config runner: truth, ROSL run, server-log trajectory read  |
| `accuracy_charts.py`  | Accuracy plots from merged `trajectory.csv`                     |