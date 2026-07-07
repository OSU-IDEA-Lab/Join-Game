# ROSL — Two-Phase, Flat-Pooled AIPW Version (with a full confidence-interval suite)

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

## What changed since the previous revision of this document

The prior draft of this readme described an adaptively-weighted estimator
whose **point estimate** was pooled with the paper's variance-stabilizing
stick-breaking / two-point weights. That is no longer the shipped behaviour.
The two changes are:

- **Flat pooling is now the shipped default (`ROSL_FLAT_WEIGHTS = 1`).**
  Per-round scores are pooled with `h_r = 1/round_pairs` — the plain per-pair
  average of round scores. The variance-stabilizing weights survive only
  behind `ROSL_FLAT_WEIGHTS = 0`, for A/B reproduction. The reason is a
  §9 post-mortem finding: inverse-variance-style weights target a
  *precision-weighted* mean, which equals the population mean only under a
  common-mean regime, but ROSL's M-blocks have genuinely different per-block
  truths, so variance-tracking weights bias the pooled estimand toward
  low-variance (cold) slices. The old proxy weights escaped that bias only by
  being nearly flat; making them exactly flat closes the exposure and measured
  equal-or-better on every tested cell. The governing principle is now
  explicit: **variance information belongs in the intervals, never in the
  weights.**
- **A whole suite of intervals is now emitted, not just one.** Alongside the
  self-normalized `ci_halfwidth`, the node now reports an M-block-clustered
  interval, a split-half within-block interval, a split-cache noise-only
  interval, a heterogeneity diagnostic (`het_ratio`), a Lindeberg telemetry
  term, and an empirical-Bernstein anytime-valid guard band (`ci_eb`) suited
  to data-dependent early stops. These are all telemetry/diagnostics or
  alternative brackets; none of them feeds back into the point estimate.

The three correctness fixes carried forward from the "Fixed-Probe" revision —
a per-state RNG, a plan-shape guard, and use of the exact outer size — are
unchanged and still apply (§3, "Correctness notes").

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
   are pooled with **flat per-pair weights** by default (`h_r = 1/round_pairs`;
   §2.3). A self-normalized variance backs a running ~95% confidence interval
   (§2.4), updated in O(1) per round, independent of the pooling-weight choice.

The bandit logic still concentrates exploitation probing on tuples with high
empirical joinability while a floor of exploration mass keeps every tuple
selectable — but because a dedicated exploration round already runs first,
exploitation doesn't need to start uniform and decay toward that floor the
way the single-phase predecessor did; `ε` is simply held fixed.

---

## 2. The estimator (two-phase AIPW, flat-pooled)

Exploration rounds score with plain Horvitz–Thompson; exploitation rounds
score with an AIPW correction on top. Every round, of either kind, is then
pooled by **flat per-pair weighting** into a single running estimate, and a
suite of intervals is maintained alongside it.

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

### 2.3 Pooling into a running estimate — flat weights by default

Rounds — exploration and exploitation alike, in the order they're produced —
are pooled with a per-round coefficient `h_r`. **The shipped default is flat:**

```
h_r = 1 / round_pairs                     (ROSL_FLAT_WEIGHTS = 1, default)
```

so the pooled estimate is the plain per-pair average of round scores:

```
μ̂ = ( Σ_r h_r · Ŷ_r ) / ( Σ_r h_r · pairs_r )
Ĵ = μ̂ · |R| · |S|
```

Why flat rather than the paper's variance-stabilizing weights: those weights
target an inverse-variance / precision-weighted mean, which coincides with the
population mean only when every round shares a common conditional mean. ROSL's
M-blocks do not — they carry genuinely different per-block truths — so
variance-tracking weights systematically bias the pooled estimand toward the
low-variance (cold) slices. The former proxy weights avoided visible bias only
by being nearly flat; making them exactly flat removes the mechanism entirely
and measured equal-or-better on every tested cell. Variance information is
still fully exploited, but it lives in the intervals (§2.4, §2.8), never in the
point estimate.

The plain, unweighted `Σ pairs_r` is still tracked (as `pairs_seen`, §4) for
diagnostics; under flat weights the weighted denominator `Σ h_r · pairs_r`
reduces to the round count.

**Legacy path (`ROSL_FLAT_WEIGHTS = 0`, A/B reproduction only).** The paper's
stick-breaking / two-point allocation is still compiled behind this switch:

```
V_r   = round_pairs / π_repr,r                (conditional-variance proxy)
λ_r   = two-point allocation rate(r; T, α)     (paper Eq. 18 / Thm. 3, clamped)
h_r²  = (stick · λ_r) / V_r,     h_r = √(h_r²)
stick ← stick − h_r² · V_r                     (residual mass left to allocate)
```

with `π_repr,r` the minimum `π(t)` over that round's cache, `α = ROSL_ALPHA`,
and `T` the estimated total round count

```
T ≈ ⌈|R|/m_lim⌉ · ( 1 + ⌈ max(|S| − n_probes, 0) / k_lim ⌉ )
```

(one exploration round plus one exploitation round per body K-block, per
M-block), with the last round forcing `λ = 1` to consume any remaining stick.
This path is retained only to reproduce the earlier variance-weighted results;
it is not the shipped estimator.

### 2.4 Confidence interval (O(1) per round)

A self-normalized variance backs a running ~95% interval around `Ĵ`,
independent of the flat-vs-legacy weight choice:

```
M1 = Σ_r h_r² Ŷ_r²,     M2 = Σ_r h_r² Ŷ_r · pairs_r,     M3 = Σ_r h_r² pairs_r²

SS = M1 − 2μ̂·M2 + μ̂²·M3                     (recentred sum of squares, König–Huygens)
V̂  = SS / ( Σ_r h_r · pairs_r )²
Ĵ ± 1.96 · |R| · |S| · √V̂                    (≈ 95% CI, reported as ci_halfwidth)
```

`M1`, `M2`, and `M3` are each updated in O(1) per round, so recomputing `SS`
at every emission avoids literally recentring every prior round's residual
against the latest `μ̂` — which would cost O(rounds) per emission and
O(rounds²) over a full run.

(The design notes' pseudocode reuses `α` for both this significance level
and the allocation-rate exponent; the implementation keeps them separate —
`ROSL_ALPHA` drives only the legacy allocation rate, and this confidence
interval is always the fixed ~95% one above, not parameterized by any GUC.)

### 2.5 |R| and |S|: planner estimate mid-run, exact at teardown

`μ̂` is exact regardless of when it's read; what changes is the population
size `|R| · |S|` used to scale it into `Ĵ`. **Mid-run** (every `ROSL_TRAJ`
row), both `|R|` and `|S|` are the planner's row-count estimates, since the
true size of `R` isn't known until the scan finishes. **At teardown**
(`ROSL_SUMM`), `|R|` is replaced by the *exact* outer count — the running sum
of every M-block's actual size (`act_outer`) — while `|S|` stays the planner's
estimate, because this version never materializes `S`: each M-block rescans it
and draws a fresh prefix, so its true cardinality is never counted directly
(see §3, "Correctness notes"). So the final `ROSL_SUMM` line is not simply the
last `ROSL_TRAJ` row restated — it's the same `μ̂` rescaled against the exact
`|R|`.

**Early-stop caveat.** `act_outer` is the *scanned* outer, exact for `|R|`
only if the outer scan actually completed (phase reached `PH_DONE`). At a
data-dependent stop (output LIMIT, client cut) the summary rescales to the
scanned-`R × S` sub-join, biased low for `J` by roughly `act_outer/|R|`, while
trajectory rows keep the planner `num_outer` scaling. A consumer that prefers
SUMM over the last trajectory row (the current harness does) inherits that
bias on early-stopped runs — compare `act_outer` against `num_outer` before
trusting SUMM there. This is exactly the regime where the empirical-Bernstein
band (§2.8) is the more defensible interval.

### 2.6 Sampling distribution: ε-greedy on joinability, fixed

```
q(t) = r(t) / a(t)     if a(t) > 0
q(t) = 1                if a(t) = 0        (untried: optimistically fully joinable)

p(t) = (1 − ε)·q(t)/Q_total + ε/|A|     if Q_total > 0
p(t) = 1/|A|                             if Q_total = 0
```

`Q_total = 0` arises when every tuple has been probed and none has ever
joined; the distribution falls back to uniform since there's no joinability
signal left to weight by.

`ε = ROSL_EPSILON` is fixed for the whole run — the exploration round already
supplies an initial signal before any exploitation round runs, so there's no
reason to start exploration-heavy and decay. The ε-floor is retained and is
now load-bearing for inference, not just a spike-damping heuristic: it
guarantees the propensity-decay lower bound the CLT (and the clustered CI)
require.

### 2.7 Split-cache replicates and the noise-only track

Each round's cache draws are tagged into two halves **A/B before dedup**. Each
half yields a half-cache score at inclusion probability `π` evaluated at
exponent `L/2`, and

```
v_r = ¼ (corr_A − corr_B)²
```

is an unbiased estimate of `Var((Y_A + Y_B)/2 | H_{r−1})` — a **pure-noise**
within-round variance estimate (it isolates cache-draw noise from the round's
true score). `v_r` feeds only the noise-only variance track
`var_noise = Σ_r h_r² v_r` and the Lindeberg telemetry `max_r h_r² v_r`
(`lindeberg_max` in the summary); it never enters a weight. Because the half
caches probe at `L/2 < L`, their `π` is smaller, so `v_r` is a conservative
(upper) gauge for the full-cache score's own noise.

### 2.8 The interval suite (all telemetry / alternative brackets)

None of these feeds back into `μ̂`. Each is O(1)-updatable.

- **`ci_halfwidth`** (§2.4) — the primary self-normalized ~95% interval.
- **`ci_noise`** — a noise-only bracket from `var_noise`; the share of the
  round-level SS attributable to cache-draw noise rather than truth.
- **`ci_clust`** — an **M-block-clustered** interval: the block, not the
  round, is the independence unit. It uses the per-block clustered residual
  `d = blk_ynum − μ·blk_wpair` with a `t`-critical value at `df = G−1`
  (`G = n_blocks`) and a `G/(G−1)` small-sample scale. It repairs the
  round-level interval's understatement of cross-block truth heterogeneity
  (at the cost of being over-wide when that heterogeneity is real). Falls back
  to `ci_halfwidth` with fewer than two blocks.
- **`ci_split`** — a **split-half within-block** interval: per-block
  `d_b = Y_A − (W_A/W_B)·Y_B` over odd/even rounds, a heterogeneity-free floor
  (`df` = number of blocks contributing both halves). Falls back to
  `ci_halfwidth` if none did.
- **`het_ratio`** — `(clust_ss · G/(G−1)) / var_noise`. A value near 1 says
  the clustered CI is honest (its width is essentially noise); a value ≫ 1
  says it is over-wide, inflated by genuine cross-block truth heterogeneity.
  `−1` means undefined (`var_noise = 0`).
- **`ci_eb` / `est_eb`** — an **empirical-Bernstein anytime-valid guard band**
  (design notes §8), Maurer–Pontil fixed-`n` form on the per-round rate
  `X = Ŷ/pairs`:

  ```
  |X̄ − μ| ≤ √(2·Vₓ·ln(2/α)/N) + (7/3)·B·ln(2/α)/N,   B = max_r |X_r|
  ```

  `est_eb` centres on the unweighted per-round mean `X̄`, which equals `μ̂`
  under the default flat weights (they differ only on the legacy weighted
  path). Requiring no variance convergence, this is the most defensible
  interval at a data-dependent stop (output LIMIT); with the plug-in range `B`
  it is a robustness heuristic there rather than a proven confidence sequence.

### 2.9 Known limitations / open points

- The round-level `ci_halfwidth` treats rounds as the independence unit and
  can understate cross-block heterogeneity; that is precisely what `ci_clust`
  and `het_ratio` exist to expose. Read them together.
- The AIPW control variate `q̂(t)` predicts *average* joinability, not this
  round's specific match count; if match density varies a lot across K-blocks,
  it helps less. Worth measuring against the plain-HT predecessor on synthetic
  sparse-join data.
- `π_repr,r` (the min-`π` proxy) affects only the legacy weighted path; under
  the shipped flat weights it is not on the estimation path.
- The exploitation distribution is still rebuilt every K-block rather than
  fixed once after exploration; whether the per-round rebuild is worth it
  relative to a distribution fixed after the exploration phase is still open.

> **Forward-looking note (not yet implemented in this code).** A separate
> design (`rosl_single_m_hadad_plan.md`) proposes a **single-M** mode: draw
> one uniform M-block once and hold it for the whole run, stream `S` in
> K-blocks with a fresh cache per round, and pool with the paper's
> adaptively-weighted AIPW machinery — the regime where those weights are
> legal, because conditional on a fixed M the round truths *do* share a common
> mean. That mode would land behind a `ROSL_SINGLE_M` switch with a
> `ROSL_WEIGHT_MODE ∈ {FLAT, HADAD_CONST, HADAD_2PT}` selector and a two-stage
> (within-M plus M-lottery) interval, and is gated on simulation results
> before any C is written. It is **not** part of the estimator described
> above; the default here remains classic two-phase sequential tiling with
> flat pooling.

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

| Constant             | Value     | Meaning                                                            |
|----------------------|-----------|-------------------------------------------------------------------|
| `ROSL_M_LIM`         | 1588      | Outer (R) tuples per M-block                                       |
| `ROSL_K_LIM`         | 1588      | Inner (body) tuples per exploitation K-block                       |
| `ROSL_EXP_CACHE_LIM` | 529       | With-replacement draws per round, either phase (`L`)              |
| `ROSL_N_PROBES`      | 1588      | Size of the exploration prefix `P` (redrawn every M-block)         |
| `ROSL_EPSILON`       | 0.2       | Fixed exploitation exploration mass (`ε`; no decay — §2.6)         |
| `ROSL_EPSILON_FLOOR` | 0.2       | Propensity-decay floor (precondition for valid inference)          |
| `ROSL_FLAT_WEIGHTS`  | 1         | 1 = flat pooling `h = 1/round_pairs` (default); 0 = legacy stick-breaking |
| `ROSL_ALPHA`         | 0.7       | Two-point allocation decay exponent (`α`) — **legacy path only**   |
| `ROSL_TRAJ_CAP`      | 1,000,000 | Max per-round trajectory rows retained for the dump                |

`ROSL_ALPHA` is read only when `ROSL_FLAT_WEIGHTS = 0`; under the default flat
pooling it has no effect. `ROSL_EPSILON_FLOOR` is retained as the propensity
floor that guarantees the CLT / clustered-CI propensity-decay bound.

### State machine

A single `ExecNestLoop` call advances a small phase machine and yields at
most one row per call (resume cursors let a round span many calls):

- **`PH_NEW_MBLOCK`** — load the next outer block; if `R` is exhausted, go to
  `PH_DONE`. Zero `reward[]` and `attempts[]` for the new block, rescan `S`,
  and load its first `n_probes` tuples into the exploration prefix `P`. On
  the first non-empty block, the per-round timing clock starts; every block
  adds to the running exact-outer-count accumulator (`act_outer`) and opens a
  new clustering block (`n_blocks`).
- **`PH_EXPLORE`** — build a *uniform* distribution over the M-block, draw
  and dedup the exploration cache (tagged into split halves A/B), and probe it
  against all of `P`, emitting matches one per call (resume cursors
  `explore_ci` / `explore_pj`). Skipped straight to `PH_NEW_SBLOCK` if `P`
  came back empty (e.g. `n_probes` exceeds `|S|`). On completion,
  `finalize_explore_round` folds the round's raw-HT score.
- **`PH_NEW_SBLOCK`** — load the next body K-block; if the body is
  exhausted, advance to `PH_NEW_MBLOCK` (closing the current block's clustered
  and split-half partials). Build the joinability-weighted, fixed-`ε`
  distribution (`q(t) = r(t)/a(t)` for probed tuples, `q(t) = 1` for untried),
  draw and dedup the exploit cache (split-tagged), and reset per-round match
  counts.
- **`PH_PROBE`** — probe the exploit cache against the K-block, emitting
  matches one per call (resume cursors `probe_ci` / `probe_kj`). When the
  round's last pair is probed, `finalize_round` folds the AIPW score into the
  flat-pooled estimate, updates the moment / noise / EB accumulators, and
  records the trajectory row (with `ci_halfwidth` and `ci_eb`), then returns
  to `PH_NEW_SBLOCK`.
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
  variance the confidence intervals are meant to report.
- **Plan-shape guard.** Covered above: falling back to the stock join rather
  than sampling under an assumption that doesn't hold trades a slower query
  for a correct one.
- **Exact outer population.** The running trajectory scales `μ̂` by the
  planner's row estimates for both `|R|` and `|S|`, since the true outer size
  isn't known mid-run. The final summary instead uses `act_outer` — the exact
  sum of every M-block's size — for `|R|`, since that's known precisely by
  teardown; `|S|` still uses the planner's estimate, because this version
  rescans `S` per M-block instead of materializing it once, and so never
  counts it exactly (§2.5).

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
ROSL_TRAJ round=<n> mean_per_pair=<μ̂> est_join=<Ĵ> ci_halfwidth=<half-width> pairs_seen=<den> sample_matches=<m> elapsed_ms=<t> ci_eb=<eb-half>
```

`ci_halfwidth` is the running self-normalized ~95% interval from §2.4;
`ci_eb` is the empirical-Bernstein anytime guard band from §2.8. `Ĵ ±
ci_halfwidth` is the primary interval; `Ĵ ± ci_eb` is the more defensible
bracket if the run is about to be cut short by an output LIMIT.

Final summary (one line at teardown):

```
ROSL_SUMM final_est_join=<Ĵ> ci_halfwidth=<half> ci_clust=<half> ci_split=<half> ci_eb=<half> est_eb=<Ĵ_eb> ci_noise=<half> het_ratio=<r> lindeberg_max=<x> n_blocks=<G> rounds=<n> sample_matches=<m> pairs_seen=<den> t_steps=<t> act_outer=<|R| exact> num_outer=<|R| planner est.> num_inner=<|S| planner est.>
```

`final_est_join` and its `ci_halfwidth` use the exact `act_outer` for `|R|`
rather than the planner's `num_outer` (§2.5); `num_outer` and `num_inner`
are logged alongside so the two can be compared. The remaining fields are the
interval suite of §2.8: `ci_clust` (M-block-clustered), `ci_split`
(split-half within-block), `ci_eb` / `est_eb` (empirical-Bernstein band and
its recentred estimate), `ci_noise` (noise-only bracket), `het_ratio`
(heterogeneity diagnostic; `-1` = undefined), `lindeberg_max` (Lindeberg
telemetry), and `n_blocks` (the number of M-blocks, i.e. the clustering `G`).

If the outer relation was empty or no round ever completed, the line instead
reads:

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
log_min_messages  = info      # INFO actually reaches the log (worker sets per session)
log_line_prefix   includes %p # per-backend PID, so concurrent workers can be told apart
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

> **Note:** the per-round log line (§4) now carries `ci_halfwidth` and
> `ci_eb`, and the summary line carries the full interval suite
> (`ci_clust`, `ci_split`, `ci_eb`/`est_eb`, `ci_noise`, `het_ratio`,
> `lindeberg_max`, `n_blocks`). If `worker.py`'s log parser wasn't updated
> alongside the estimator change, add these fields (and, if useful, derived
> `ci_lower` / `ci_upper`) to the merged schema so interval coverage can be
> checked against `truth` the same way `accuracy_charts.py` already checks
> `rel_error`. Since the schema is additive, older per-job CSVs won't merge
> with new runs — the manager's header-mismatch guard fails loudly, so give
> each sweep a fresh results directory.

---

## 6. File map

| File                  | Role                                                                                              |
|-----------------------|---------------------------------------------------------------------------------------------------|
| `nodeNestloop.c`      | ROSL executor node (two-phase sampling join + flat-pooled AIPW estimator + CI suite + log dump)  |
| `tpch_manager.py`     | Sweep manager: fan-out, timing, CSV merge                                                          |
| `worker.py`           | Per-config runner: truth, ROSL run, server-log trajectory read                                    |
| `accuracy_charts.py`  | Accuracy plots from merged `trajectory.csv`                                                        |