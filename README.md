# ROSL — Alternative Version, Epsilon Greedy on Observed Joinability

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

Given outer relation `R` and inner relation `S`, ROSL:

1. Consumes `R` in **M-blocks** of `m_lim` tuples. For each M-block it scans all
   of `S` in **K-blocks** of `k_lim` tuples. Each (M-block, K-block) pair is a
   **round**.
2. Within a round it builds a non-uniform probability distribution over the
   M-block's tuples from accumulated **average joinability** — each tuple's
   cumulative match count divided by its cumulative probe count; untried tuples
   are optimistically assigned full joinability (1) — blended with an
   exploration term controlled by `ε`.
3. Draws `exp_cache_lim` tuples **with replacement** from that distribution,
   deduplicates them into an **exploit cache**, and probes the cache against the
   current K-block, emitting matches as output rows.
4. Updates match counts and probe attempt counters, and in the estimator
   variant folds a per-round **Horvitz–Thompson** estimate into a running
   total to produce `Ĵ`, the estimated join size.

The bandit logic concentrates probing on tuples with high empirical joinability
(exploitation) while retaining a floor of random exploration so every tuple
keeps a non-zero chance of being sampled.

---

## 2. The estimator (Horvitz–Thompson)

ROSL's estimate is unbiased-by-design through inverse-probability weighting.

### Per-round estimate

For round `(i, j)` — M-block `Mᵢ` against K-block `Kⱼ` — with deduplicated
exploit cache `Cᵢⱼ`:

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

### Pooling rounds into a running estimate

Because outer blocks partition `R` and each is probed against all of `S`, the
slices `Mᵢ × Kⱼ` are disjoint and tile `R × S`. The per-round estimates pool
into a single mean, scaled by the known pair count:

```
μ̂ = ( Σ_{(i,j) ∈ P} Ŷ_ij ) / ( Σ_{(i,j) ∈ P} |Mᵢ|·|Kⱼ| )
Ĵ = μ̂ · |R| · |S|
```

`μ̂` is the estimated mean join successes per tuple-pair over all pairs processed
so far; `Ĵ` is the running estimate of `|R ⋈ S|`. No per-block weight is needed
beyond the pair counts already in the denominator.

### Sampling distribution (custom ε-smoothing)

For each tuple `t` in M-block `A`, define its **average joinability**:

```
q(t) = r(t) / a(t)     if a(t) > 0
q(t) = 1               if a(t) = 0   (untried: optimistically fully joinable)
```

where `r(t)` is cumulative match successes and `a(t)` is cumulative probe
attempts (incremented by `|K|` each round `t` appears in the cache). With
`Q_total = Σ_{t ∈ A} q(t)`:

```
p(t) = (1 − ε)·q(t)/Q_total + ε/|A|     if Q_total > 0
p(t) = 1/|A|                             if Q_total = 0
```

`Q_total = 0` arises when every tuple has been probed and none has ever
joined; the distribution falls back to uniform since there is no joinability
signal to weight by. The construction sums to 1 over `A` in both cases.

### Epsilon floor

`ε` starts at 1 (full exploration) at each new M-block and decays **toward a
floor** at the end of every round:

```
ε ← (ε + ε_floor) / 2
```

This is asymptotic: `ε` approaches but never drops below `ε_floor ∈ (0, 1]`.
Holding `ε` above the floor keeps every `p(t) ≥ ε_floor/|A|`, so no tuple's
selection probability collapses toward 0. This matters for the estimator: a
vanishing `p(t)` would make `π(t)` saturate and the inverse-probability weight
`1/π(t)` blow up, producing large spikes whenever a near-zero-probability tuple
happens to be selected and match. The floor bounds the weights and keeps the
estimate stable in late, exploit-heavy rounds.

**Known limitations of the current sampler** (from the design notes):

- The distribution is rebuilt every round rather than fixed once after an
  exploration phase. This allows exploitation to track joinability more closely
  as trials accumulate, but carries a per-round cost whose justification is
  still under investigation.

The earlier reward-based version also had the weakness that never-sampled
tuples were indistinguishable from observed zero-reward tuples, biasing the
sampler toward re-selecting already-seen tuples. The joinability distribution
addresses this by assigning `q(t) = 1` to untried tuples — treating them as
optimistically fully joinable — so they compete on equal footing with
high-reward tuples during early exploration.

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

| Constant              | Value     | Meaning                                              |
|-----------------------|-----------|------------------------------------------------------|
| `ROSL_M_LIM`          | 1588      | Outer (R) tuples per M-block                          |
| `ROSL_K_LIM`          | 1588      | Inner (S) tuples per K-block                          |
| `ROSL_EXP_CACHE_LIM`  | 529       | With-replacement draws per round (`L`)                |
| `ROSL_EPSILON_FLOOR`  | 0.2       | Minimum exploration threshold (`ε_floor`)             |
| `ROSL_TRAJ_CAP`       | 1,000,000 | Max per-round trajectory rows retained for the dump   |

### State machine

A single `ExecNestLoop` call advances a small phase machine and yields at most
one row per call (resume cursors let a round span many calls):

- **`PH_NEW_MBLOCK`** — load the next outer block; if `R` is exhausted, go to
  `PH_DONE`. Zero both `reward[]` and `attempts[]` for the new block, reset
  `ε` to 1 (full exploration), and rewind the inner relation. On the first
  non-empty block, the per-round timing clock starts.
- **`PH_NEW_SBLOCK`** — load the next inner block; build the
  joinability-weighted distribution (`q(t) = r(t)/a(t)` for probed tuples,
  `q(t) = 1` for untried) blended with exploration mass `ε`; draw and
  deduplicate the exploit cache; advance `a(t)` by `|K|` for every cached
  tuple (before probing, so inclusion probabilities depend only on prior
  rounds); reset per-round match counts.
- **`PH_PROBE`** — probe the exploit cache against the K-block, emitting matches
  one per call. When the round's last pair is probed, fold the Horvitz–Thompson
  estimate (`finalize_round`), decay `ε`, advance to the next K-block.
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

Per-round trajectory (one line per round):

```
ROSL_TRAJ round=<n> mean_per_pair=<μ̂> est_join=<Ĵ> pairs_seen=<den> sample_matches=<m> elapsed_ms=<t>
```

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

---

## 6. File map

| File                  | Role                                                            |
|-----------------------|-----------------------------------------------------------------|
| `nodeNestloop.c`      | ROSL executor node (sampling join + HT estimator + log dump)    |
| `tpch_manager.py`     | Sweep manager: fan-out, timing, CSV merge                       |
| `worker.py`           | Per-config runner: truth, ROSL run, server-log trajectory read  |
| `accuracy_charts.py`  | Accuracy plots from merged `trajectory.csv`                     |
