# ROSL — Adaptively-Weighted, Two-Phase Version (AIPW on Observed Joinability)

A sampling-based join operator for PostgreSQL that replaces the inner loop
of the executor's nested-loop node with a two-phase, joinability-guided
(bandit-style) sampler, and produces a running, confidence-interval-backed
estimate of the total join size as it streams matching rows. ROSL trades
exactness for early, progressively-accurate estimates: it can report an
approximate `|R ⋈ S|` — with an error bar — long before a full join would
finish.

This document describes both the algorithm (design notes §7 — "Adaptively-
Weighted Probabilistic N-failure," which keeps the two-phase sampling
procedure introduced in §6 and replaces only the estimator, after Hadad,
Hirshberg, Zhan, Wager & Athey's *Confidence Intervals for Policy Evaluation
in Adaptive Experiments*) and the PostgreSQL implementation in
`nodeNestloop.c`, including how the estimator communicates results and how
the benchmark harness consumes them.

## What changed since the single-phase version

This supersedes the single-phase, uniform-pooled Horvitz–Thompson version
previously documented here (design notes §4). Two changes to the algorithm
itself:

- **Sampling is now two-phase per M-block.** A shared exploration prefix `P`
  — the first `n_probes` tuples of a freshly rescanned `S` — is probed once
  per M-block with a uniform cache (§§1–2.2). The remaining body `B = S \ P`
  is still scanned in K-blocks, but the exploitation `ε` no longer decays
  (§2.6).
- **The estimator is now adaptively-weighted AIPW.** Exploitation rounds
  score via an AIPW control variate instead of raw HT (§2.2); every round —
  exploration and exploitation alike — pools with variance-stabilizing
  weights instead of a uniform sum (§2.3); and a self-normalized variance
  now backs a running ~95% confidence interval (§2.4).

Plus three correctness fixes, carried forward from an intermediate
"Fixed-Probe" revision (§3, "Correctness notes"): a proper per-state RNG, a
plan-shape guard that falls back to the exact stock join when ROSL's
assumptions don't hold, and use of the *exact* outer relation size — rather
than the planner's estimate — in the final summary.

---

## 1. What ROSL does

Given outer relation `R` and inner relation `S`, ROSL:

1. Consumes `R` in **M-blocks** of `m_lim` tuples. For each M-block it
   rescans `S` from the top; the first `n_probes` tuples become a fresh
   **exploration prefix** `P`, and the remainder is the **exploitation body**
   `B = S \ P`, consumed in **K-blocks** of `k_lim` tuples. `P` is redrawn —
   via a fresh rescan — for every M-block, and is disjoint from that
   M-block's body blocks.
2. Runs one **exploration round** per M-block: draws `exp_cache_lim` tuples
   **with replacement**, *uniformly* over the M-block, deduplicates them into
   a cache, and probes that cache against every tuple in `P`, emitting
   matches. This round's score is a raw Horvitz–Thompson estimate of matches
   in `M × P`.
3. Runs one **exploitation round** per K-block: builds a non-uniform
   probability distribution over the M-block from accumulated **average
   joinability** — each tuple's cumulative match count divided by its
   cumulative probe count (attempts made in exploration and exploitation
   both count); untried tuples are optimistically assigned full joinability
   (1) — blended with a **fixed** exploration mass `ε`. Draws and
   deduplicates another `exp_cache_lim`-tuple cache from that distribution,
   probes it against the current K-block, and emits matches. This round's
   score is an **AIPW** (augmented inverse-probability-weighted) estimate
   over `M × K`, using the block's frozen match rate as a control variate.
4. Folds **every** round — exploration and exploitation alike, in the order
   produced — into one running estimate `Ĵ` of the total join size. Rounds
   are pooled with variance-stabilizing weights (a stick-breaking, two-point
   allocation scheme) rather than a uniform average, which keeps the pooled
   estimator asymptotically normal and lets ROSL maintain a genuine ~95%
   confidence interval alongside `Ĵ`, updated in O(1) per round.

The bandit logic still concentrates exploitation probing on tuples with high
empirical joinability while a floor of exploration mass keeps every tuple
selectable — but because a dedicated exploration round already runs first,
exploitation doesn't need to start uniform and decay toward that floor the
way the single-phase predecessor did; `ε` is simply held fixed.

---

## 2. The estimator (adaptively-weighted AIPW)

Exploration rounds score with plain Horvitz–Thompson; exploitation rounds
score with an AIPW correction on top. Every round, of either kind, is then
pooled using the same variance-stabilizing weights, so there is exactly one
running estimate and one running confidence interval.

### 2.1 Two regions per M-block

Each M-block's pass over `S` is split into two disjoint regions:

- **`M × P`** — the exploration region, probed once per M-block against the
  freshly-loaded prefix `P`.
- **`M × B`** — the exploitation region, probed across the K-blocks of the
  body `B = S \ P`.

Because `P` is reloaded from a fresh rescan of `S` for *every* M-block
(rather than drawn once and shared globally across all M-blocks, as in an
earlier "Fixed-Probe" design), the disjointness between `P` and `B` holds
within a single M-block's pass, not across the whole run.

### 2.2 Per-round score

**Exploration round** — raw Horvitz–Thompson, since untried tuples carry no
joinability signal yet to use as a control variate:

```
C_exp ~ Uniform(M), |C_exp| ≤ L, drawn with replacement and deduplicated
π_exp = 1 − (1 − 1/|M|)^L                (identical for every t: draws are uniform)
Ŷ_exp = Σ_{t ∈ C_exp}  matches(t, P) / π_exp
```

estimating total matches in `M × P`, with `round_pairs = |M| · |P|`.

**Exploitation round** — AIPW, using the block's frozen match rate as a
control variate:

```
q̂(t) = r(t) / a(t)     if a(t) > 0     (frozen BEFORE the round is probed)
q̂(t) = 0                if a(t) = 0     (untried: predict ZERO matches)

Ŷ_round = Σ_{t ∈ M} q̂(t)·|K|                              (baseline, all of M)
        + Σ_{t ∈ C} ( matches(t) − q̂(t)·|K| ) / π(t)       (correction, cache only)
```

where `C` is the round's exploit cache, drawn from the ε-greedy-on-joinability
distribution (§2.6), and `π(t) = 1 − (1 − p(t))^L` its inclusion probability.
`round_pairs = |M| · |K|`.

`q̂(t)` is a genuine regression *predictor* of this round's match count, which
is why it treats untried tuples pessimistically (`q̂(t) = 0`) — the opposite
of the *selection* weight `q(t)`, which treats them optimistically
(`q(t) = 1`, §2.6) so they still get explored. Using the optimistic value as
the baseline would claim every untried tuple matches all of `K`, and the
correction term would then subtract that back with a large `1/π` factor,
injecting exactly the variance the control variate exists to remove. Because
most tuple pairs don't join, `q̂(t) ≈ 0` for most tuples, the baseline stays
small, and the `1/π` correction fires only on the rare tuples that actually
match — which is where most of the estimator's variance under sparse joins
used to come from.

### 2.3 Pooling into a running, adaptively-weighted estimate

Rounds — exploration and exploitation alike, in the order they're produced —
no longer pool by uniform summation. Instead each round `r` is weighted by a
coefficient `h_r` chosen via the stick-breaking / two-point allocation scheme
of Hadad, Hirshberg, Zhan, Wager & Athey. This matters because the sampling
distribution in round `r` depends on rewards observed in rounds `1..r-1` — the
data is *adaptively collected* in exactly the sense the paper addresses, and
while a uniformly-pooled HT estimator over adaptive data stays unbiased, it
isn't guaranteed asymptotically normal, and its variance can be dominated by
a handful of rare, low-`π` tuples that happen to match — the same spikes a
fixed ε-floor was previously used to dampen.

For round `r` with pair count `pairs_r` and representative inclusion
probability `π_repr,r` (the *minimum* `π(t)` over that round's cache — a round
is only as reliable as its most fragile included tuple):

```
V_r   = pairs_r / π_repr,r                    (conditional-variance proxy)
λ_r   = two-point allocation rate(r; T, α)     (paper Eq. 18 / Thm. 3, clamped)
h_r²  = (stick · λ_r) / V_r,     h_r = √(h_r²)
stick ← stick − h_r² · V_r                     (residual mass left to allocate)
```

`stick` starts at 1 and is monotonically consumed round by round. `λ_r`
blends a "stays high" branch `1/(T−r+1)` with a decaying branch
`r^−α / ( r^−α + (T^(1−α) − r^(1−α))/(1−α) )`, mixed by `π_repr,r` (a safer,
high-`π` round leans toward the constant branch; a fragile round leans toward
the decaying one) and clamped into bounds derived from those same two
branches; `α = ROSL_ALPHA`. `T` is the *estimated* total round count,
computed once up front from the planner's row estimates and block sizes —

```
T ≈ ⌈|R|/m_lim⌉ · ( 1 + ⌈ max(|S| − n_probes, 0) / k_lim ⌉ )
```

— i.e. one exploration round plus one exploitation round per body K-block,
for every M-block. The last round forces `λ = 1` so any remaining stick is
fully allocated.

Each round then folds into the running point estimate:

```
μ̂ = ( Σ_r h_r · Ŷ_r ) / ( Σ_r h_r · pairs_r )
Ĵ = μ̂ · |R| · |S|
```

the same shape as the plain-HT pooling in the single-phase predecessor,
except the sum is weighted by `h_r` instead of `1`. The plain, unweighted
`Σ pairs_r` is still tracked (as `pairs_seen`, §4) for diagnostics, but the
point estimate itself divides by the *weighted* pair mass, not the raw one.

### 2.4 Confidence interval (O(1) per round)

Because `h_r` is chosen so the pooled conditional variance converges, the
resulting statistic is asymptotically normal, so ROSL can maintain a real
interval alongside the point estimate, not just `Ĵ` on its own:

```
M1 = Σ_r h_r² Ŷ_r²,     M2 = Σ_r h_r² Ŷ_r · pairs_r,     M3 = Σ_r h_r² pairs_r²

SS = M1 − 2μ̂·M2 + μ̂²·M3                     (recentred sum of squares, König–Huygens)
V̂  = SS / ( Σ_r h_r · pairs_r )²
Ĵ ± 1.96 · |R| · |S| · √V̂                    (≈ 95% CI)
```

`M1`, `M2`, and `M3` are each updated in O(1) per round, so recomputing `SS`
at every emission avoids literally recentring every prior round's residual
against the latest `μ̂` — which would cost O(rounds) per emission and
O(rounds²) over a full run.

(The design notes' pseudocode reuses `α` for both this significance level
and the allocation-rate exponent in §2.3; the implementation keeps them
separate — `ROSL_ALPHA` drives only the allocation rate, and this confidence
interval is always the fixed ~95% one above, not parameterized by
`ROSL_ALPHA` or any GUC.)

### 2.5 |R| and |S|: planner estimate mid-run, exact at teardown

`μ̂` is exact regardless of when it's read; what changes is the population
size `|R| · |S|` used to scale it into `Ĵ`. **Mid-run** (every `ROSL_TRAJ`
row), both `|R|` and `|S|` are the planner's row-count estimates, since the
true size of `R` isn't known until the scan finishes. **At teardown**
(`ROSL_SUMM`), `|R|` is replaced by the *exact* outer count — the running sum
of every M-block's actual size — while `|S|` stays the planner's estimate,
because this version never materializes `S`: each M-block rescans it and
draws a fresh prefix, so its true cardinality is never counted directly (see
§3, "Correctness notes"). So the final `ROSL_SUMM` line is not simply the
last `ROSL_TRAJ` row restated — it's the same `μ̂` rescaled against the exact
`|R|`.

### 2.6 Sampling distribution: ε-greedy on joinability, now fixed

Unchanged in shape from the single-phase predecessor:

```
q(t) = r(t) / a(t)     if a(t) > 0
q(t) = 1                if a(t) = 0        (untried: optimistically fully joinable)

p(t) = (1 − ε)·q(t)/Q_total + ε/|A|     if Q_total > 0
p(t) = 1/|A|                             if Q_total = 0
```

`Q_total = 0` arises when every tuple has been probed and none has ever
joined; the distribution falls back to uniform since there's no joinability
signal left to weight by.

What's different: **ε no longer decays.** In the single-phase predecessor,
`ε` started at 1 (pure exploration) at the top of each M-block and walked
toward a floor after every round. Here `ε = ROSL_EPSILON` is fixed for the
whole run — the exploration round already supplies an initial signal before
any exploitation round runs, so there's no reason to start exploration-heavy
and decay. A constant `ε` is now load-bearing for correctness, not just a
spike-damping heuristic: the confidence interval in §2.4 relies on a
propensity-decay bound that a decaying `ε` would not guarantee. (The old
`ROSL_EPSILON_FLOOR` constant is still defined in the source for reference
but is no longer read by any code path — see §3.)

### 2.7 Known limitations / open points (from the design notes)

- `π_repr,r` is a heuristic stand-in for a per-observation propensity: the
  paper's framework is defined per-observation, but ROSL's rounds are
  batches over a cache of tuples with heterogeneous `π(t)`, and taking the
  minimum is a conservative, worst-case-fragility choice. A per-tuple
  weighting variant (inside the round, not once per round) would likely be
  tighter, at the cost of more bookkeeping.
- The AIPW control variate `q̂(t)` predicts *average* joinability, not this
  round's specific match count; if match density varies a lot across
  K-blocks, it helps less. Worth measuring against the plain-HT predecessor
  on synthetic sparse-join data.
- The two-point allocation rate depends on `T`, the estimated total round
  count, computed once up front from the planner's estimates. Any reasonable
  approximation preserves unbiasedness; a better one only improves
  efficiency.
- The exploitation distribution is still rebuilt every K-block rather than
  fixed once after exploration; the per-round rebuild cost — and whether
  it's worth it relative to a distribution fixed after the exploration
  phase — is still an open question.

---

## 3. Implementation (`nodeNestloop.c`)

ROSL is implemented inside PostgreSQL's nested-loop executor node. The stock
node and ROSL share the same `NestLoopState`; ROSL's working state
(`RoslJoinState`) is allocated lazily the first time it runs.

### Behaviour is gated by a GUC — and a plan-shape guard

```
SET enable_rosl = off;   -- exact stock nested loop (default)
SET enable_rosl = on;    -- ROSL sampling join + running estimate, if the plan allows it
```

With `enable_rosl = off`, the node behaves exactly like the unmodified nested
loop — no overhead, no behavioural change. Registering the boolean GUC
requires the usual entry in `guc.c`; the node also needs a `void *rosl;`
field added to `NestLoopState` in `execnodes.h`.

Even with `enable_rosl = on`, `ExecNestLoop` falls back to the exact stock
path unless the join is a plain **inner** join with a **non-parameterized**
inner (`nestParams == NIL`). ROSL rescans the whole inner per M-block and
emits inner-join matches only, so running it against a parameterized
nestloop or an outer join would silently produce wrong results — the guard
takes priority over the GUC. The experimental two-table inner-join queries
this estimator targets always satisfy it, but it's worth checking `EXPLAIN`
for a parameterized inner or a non-inner join type if a query unexpectedly
runs at stock-nested-loop speed with `enable_rosl = on`. (This is part of
why §5's plan-shaping `SET`s exist — they're what keep the chosen plan
inside the guard's assumptions.)

### Tunable constants

Defined at the top of the file:

| Constant             | Value     | Meaning                                                         |
|-----------------------|-----------|-------------------------------------------------------------------|
| `ROSL_M_LIM`          | 1588      | Outer (R) tuples per M-block                                      |
| `ROSL_K_LIM`          | 1588      | Inner (body) tuples per exploitation K-block                      |
| `ROSL_EXP_CACHE_LIM`  | 529       | With-replacement draws per round, either phase (`L`)               |
| `ROSL_N_PROBES`       | 1588      | Size of the exploration prefix `P` (redrawn every M-block)         |
| `ROSL_EPSILON`        | 0.2       | Fixed exploitation exploration mass (`ε`; no decay — §2.6)         |
| `ROSL_ALPHA`          | 0.7       | Two-point allocation decay exponent (`α`, Hadad et al. Eq. 18)     |
| `ROSL_TRAJ_CAP`       | 1,000,000 | Max per-round trajectory rows retained for the dump                |

`ROSL_EPSILON_FLOOR` (0.2) is still `#define`d in the source but is no longer
referenced anywhere — a holdover from the single-phase predecessor's
decaying-`ε` design, superseded here by the fixed `ROSL_EPSILON`.

### State machine

A single `ExecNestLoop` call advances a small phase machine and yields at
most one row per call (resume cursors let a round span many calls):

- **`PH_NEW_MBLOCK`** — load the next outer block; if `R` is exhausted, go to
  `PH_DONE`. Zero `reward[]` and `attempts[]` for the new block, rescan `S`,
  and load its first `n_probes` tuples into the exploration prefix `P`. On
  the first non-empty block, the per-round timing clock starts; every block
  adds to the running exact-outer-count accumulator (`act_outer`).
- **`PH_EXPLORE`** — build a *uniform* distribution over the M-block, draw
  and dedup the exploration cache, and probe it against all of `P`, emitting
  matches one per call (resume cursors `explore_ci` / `explore_pj`). Skipped
  straight to `PH_NEW_SBLOCK` if `P` came back empty (e.g. `n_probes`
  exceeds `|S|`). On completion, `finalize_explore_round` folds the round's
  raw-HT score and moves on.
- **`PH_NEW_SBLOCK`** — load the next body K-block; if the body is
  exhausted, advance to `PH_NEW_MBLOCK`. Build the joinability-weighted,
  fixed-`ε` distribution (`q(t) = r(t)/a(t)` for probed tuples, `q(t) = 1`
  for untried), draw and dedup the exploit cache, and reset per-round match
  counts.
- **`PH_PROBE`** — probe the exploit cache against the K-block, emitting
  matches one per call (resume cursors `probe_ci` / `probe_kj`). When the
  round's last pair is probed, `finalize_round` folds the AIPW score into
  the adaptively-weighted estimate and records the trajectory row (with
  CI), then returns to `PH_NEW_SBLOCK` for the next body block.
- **`PH_DONE`** — outer relation exhausted; the cursor returns NULL.

Completion is signalled to the client purely by the cursor returning NULL —
no mid-stream message is emitted (see §4).

### Correctness notes

Three things matter for trusting the numbers this version produces:

- **Per-state RNG.** Cache draws use an `xorshift64*` stream seeded per
  `RoslJoinState` from a mix of high-resolution time, the backend PID, and
  the state pointer — not a process-global `rand()`/`srand()`. A shared,
  second-resolution `srand(time(NULL))` seed would hand identically-timed
  concurrent benchmark workers identical "random" caches, correlating runs
  meant to be independent replicates and biasing exactly the cross-run
  variance the confidence interval (§2.4) is meant to report.
- **Plan-shape guard.** Covered above: falling back to the stock join
  rather than sampling under an assumption that doesn't hold trades a
  slower query for a correct one.
- **Exact outer population.** The running trajectory scales `μ̂` by the
  planner's row estimates for both `|R|` and `|S|`, since the true outer
  size isn't known mid-run. The final summary instead uses `act_outer` —
  the exact sum of every M-block's size — for `|R|`, since that's known
  precisely by teardown; `|S|` still uses the planner's estimate, because
  this version rescans `S` per M-block instead of materializing it once,
  and so never counts it exactly (§2.5).

---

## 4. Output protocol — server-log only

ROSL does **not** stream estimates back to the client mid-query. Instead it
accumulates the entire per-round trajectory in its executor state and dumps
it **once, at executor teardown** (`ExecEndNestLoop` → `PrintRoslCounters`),
as `elog(INFO, ...)` lines to the **PostgreSQL server log**. Row delivery and
measurement delivery are on separate channels.

This decoupling is deliberate: an earlier design emitted a per-round
`NOTICE` during row production, which deadlocked a server-side cursor
because notices are only flushed at fetch boundaries. With the log-only
dump, nothing the client must parse is interleaved with rows, so that class
of hang is structurally impossible.

### Log line formats

Per-round trajectory (one line per round — exploration and exploitation
rounds are both recorded, in the order they occur):

```
ROSL_TRAJ round=<n> mean_per_pair=<μ̂> est_join=<Ĵ> ci_halfwidth=<half-width> pairs_seen=<den> sample_matches=<m> elapsed_ms=<t>
```

`ci_halfwidth` is new relative to the single-phase predecessor: `Ĵ ±
ci_halfwidth` is the running ~95% confidence interval from §2.4.

Final summary (one line at teardown):

```
ROSL_SUMM final_est_join=<Ĵ> ci_halfwidth=<half-width> rounds=<n> sample_matches=<m> pairs_seen=<den> t_steps=<t> act_outer=<|R| exact> num_outer=<|R| planner est.> num_inner=<|S| planner est.>
```

`final_est_join` and its `ci_halfwidth` use the exact `act_outer` for `|R|`
rather than the planner's `num_outer` (§2.5); `num_outer` and `num_inner`
are logged alongside so the two can be compared. If the outer relation was
empty or no round ever completed, the line instead reads:

```
ROSL_SUMM final_est_join=0.00 rounds=0 sample_matches=<m> (no completed rounds: outer empty or join produced no pairs)
```

If a run exceeds `ROSL_TRAJ_CAP` rounds, the first `ROSL_TRAJ_CAP` are kept
and a `ROSL_TRAJ truncated: ...` line is emitted (use a smaller scale
factor).

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

ROSL must run as a plain, fixed-inner nested loop — the shape the guard in
§3 requires to actually engage rather than silently reverting to the stock
join. The benchmark worker sets (among others):

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

> **Note:** the per-round log line (§4) now also carries a `ci_halfwidth`
> field that isn't in this column list yet. If `worker.py`'s log parser
> wasn't updated alongside the estimator change, add `ci_halfwidth` (and,
> if useful, derived `ci_lower` / `ci_upper`) to the merged schema so CI
> coverage can be checked against `truth` the same way `accuracy_charts.py`
> already checks `rel_error`.

---

## 6. File map

| File                  | Role                                                                                                |
|------------------------|-------------------------------------------------------------------------------------------------------|
| `nodeNestloop.c`      | ROSL executor node (two-phase sampling join + adaptively-weighted AIPW estimator + CI + log dump)  |
| `tpch_manager.py`     | Sweep manager: fan-out, timing, CSV merge                                                          |
| `worker.py`           | Per-config runner: truth, ROSL run, server-log trajectory read                                     |
| `accuracy_charts.py`  | Accuracy plots from merged `trajectory.csv`                                                        |