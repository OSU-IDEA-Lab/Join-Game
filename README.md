# ROSL — Flat-Weighted AIPW with Split-Cache Variance Decomposition

A sampling-based join operator for PostgreSQL that replaces the inner loop
of the executor's nested-loop node with a two-phase, joinability-guided
(bandit-style) sampler, and produces a running, confidence-interval-backed
estimate of the total join size as it streams matching rows. ROSL trades
exactness for early, progressively-accurate estimates: it can report an
approximate `|R ⋈ S|` — with an error bar — long before a full join would
finish.

This document describes the estimator **exactly as it runs in
`nodeNestloop.c`** under the shipped default `ROSL_FLAT_WEIGHTS = 1`,
tied to the struct fields and functions that implement it. It corresponds to
Section 8 of the design notes ("Flat-Weighted AIPW with Split-Cache Variance
Decomposition"), which is Section 7's adaptively-weighted estimator with the
two shipped refinements folded in — flat pooling weights (change 1′) and a
split-cache within-round variance track (change 3′) — and the legacy
stick-breaking branch left compiled out. The sampling procedure is the
probabilistic-N-failure / ε-greedy-on-joinability procedure of Section 6,
unchanged; only the estimator differs.

## What "flat-weighted" means, and what changed

Two facts define this build relative to the adaptively-weighted estimator it
descends from:

- **Pooling weights are flat (`h_r = 1/pairs_r`).** The pooled point estimate
  is the plain per-pair average of round scores; the weights carry no variance
  information. The paper's variance-stabilizing stick-breaking / two-point
  weights survive only behind `ROSL_FLAT_WEIGHTS = 0`, for A/B reproduction.
  The reason is the adaptive-weights post-mortem: inverse-variance weighting
  targets a *precision-weighted* mean, which equals the population mean only
  under a common conditional mean across rounds. ROSL's M-blocks tile `R` and
  therefore have genuinely different per-block truths, so variance-tracking
  weights bias the pooled estimand toward low-variance (typically cold,
  low-density) slices. The former proxy weights escaped visible bias only by
  being nearly flat in practice; making them exactly flat closes the exposure
  and measured equal-or-better on every tested cell. The governing rule:
  **variance information belongs in the intervals, never in the weights.**
- **A split-cache within-round variance is computed every round.** The `L`
  with-replacement cache draws are split into two contiguous halves before
  dedup; each half yields an independent AIPW replicate, and their squared
  difference is an unbiased, noise-only per-round variance `v_r`. It costs two
  extra arithmetic lines and never touches the point estimate — it feeds only
  the interval-side telemetry.

Every variance construct in the node — the König–Huygens moment accumulators,
the split-cache noise track, and the block-clustered / split-half brackets —
therefore lives purely on the interval side. The three correctness fixes
carried from the Fixed-Probe baseline (per-state RNG, plan-shape guard, exact
outer size) are unchanged and still apply (§3, "Correctness notes").

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
   in `M × P` (untried tuples have no joinability signal yet, so there is no
   control variate).
3. Runs one **exploitation round** per K-block: builds a non-uniform
   probability distribution over the M-block from accumulated **average
   joinability** — each tuple's cumulative match count divided by its
   cumulative probe count (exploration and exploitation both count); untried
   tuples are optimistically assigned full joinability (`q(t) = 1`) — blended
   with a **fixed** exploration mass `ε`. Draws and deduplicates another
   `exp_cache_lim`-tuple cache from that distribution, probes it against the
   current K-block, and emits matches. This round's score is an **AIPW**
   (augmented inverse-probability-weighted) estimate over `M × K`, using a
   frozen per-tuple match-rate predictor as the control variate.
4. Folds **every** round — exploration and exploitation alike, in the order
   produced — into one running estimate `Ĵ` of the total join size, pooled
   with **flat per-pair weights** (`h_r = 1/pairs_r`). A self-normalized
   variance backs a running ~95% confidence interval, and a family of
   alternative intervals and diagnostics is maintained alongside it, all in
   O(1) per round.

The bandit logic concentrates exploitation probing on tuples with high
empirical joinability while a floor of exploration mass (`ε`) keeps every
tuple selectable. Because a dedicated exploration round runs first, `ε` is
held fixed rather than decayed — a constant `ε` is also load-bearing for
inference, since it bounds every selection probability below by `ε/|A|`,
keeping every inclusion probability positive so the AIPW correction and both
half-cache corrections are always well-defined.

---

## 2. The estimator (flat-weighted AIPW)

Exploration rounds score with raw Horvitz–Thompson; exploitation rounds score
with an AIPW control-variate correction on top. Every round, of either kind,
is pooled by flat per-pair weighting into a single running estimate, and a
suite of intervals is maintained around it. None of the interval machinery
feeds back into the point estimate.

### 2.1 Two regions per M-block

Each M-block's pass over `S` is split into two disjoint regions:

- **`M × P`** — the exploration region, probed once per M-block against the
  freshly-loaded prefix `P`.
- **`M × B`** — the exploitation region, probed across the K-blocks of the
  body `B = S \ P`.

Because `P` is reloaded from a fresh rescan of `S` for *every* M-block, the
disjointness between `P` and `B` holds within a single M-block's pass, not
across the whole run. Both regions are Horvitz–Thompson / AIPW estimated;
there is no exact count in this design.

### 2.2 Per-round score (AIPW, baseline-plus-correction form)

The selection value `q(t)` and the AIPW predictor `q̂(t)` coincide for tried
tuples and deliberately diverge for untried ones. Selection stays optimistic
(`q(t) = 1`) to preserve exploration bandwidth; the score predicts `q̂(t) = 0`.
The optimism must never leak into the score — substituting `q` for `q̂` would
inflate the baseline by `|{t : a(t)=0}|·|K|` every round until those tuples are
probed, which under adaptive weighting ossifies into finite-sample bias. Both
predictors are **frozen** into `st->qhat[t]` / `st->q[t]` in
`build_distribution` *before* the round's cache is drawn, so they are
`H_{r-1}`-measurable.

```
q̂(t) = r(t) / a(t)     if a(t) > 0     (frozen match-rate predictor)
q̂(t) = 0                if a(t) = 0     (untried: predict ZERO matches)
```

**Exploitation round** — AIPW, computed in `finalize_round` in the
algebraically identical baseline-plus-correction form so the baseline is
accumulated once over all of `M` and only the residual correction is scanned
over the (small) cache:

```
Ŷ_r = Σ_{t ∈ M}  q̂(t)·|K|                                (base, over all of M)
    + Σ_{t ∈ C}  ( match(t) − q̂(t)·|K| ) / π(t)          (corr, over the cache)

π(t) = 1 − (1 − p(t))^L                                  (inclusion probability)
```

Because most tuple pairs don't join, `q̂(t) ≈ 0` for most tuples, the baseline
stays small, and the `1/π` correction fires only on the rare tuples that
actually match — which is where most of the estimator's variance under sparse
joins used to come from. This is the change that most directly targets the
sparse-join concern.

**Exploration round** — raw HT (`finalize_explore_round`), no control variate:

```
Ŷ_r = Σ_{t ∈ C} match(t) / π_exp,     π_exp = 1 − (1 − 1/|M|)^L
```

`round_pairs = |M|·|P|` for exploration, `|M|·|K|` for exploitation.

### 2.3 Pooling into a running estimate — flat weights

Each round contributes `h_r · Ŷ_r` with `h_r = 1/pairs_r`, so the pooled
estimate is the plain per-pair average of round scores:

```
μ̂ = est_num / weight_sum = ( Σ_r Ŷ_r/pairs_r ) / R      (R = round count)
Ĵ = μ̂ · act_outer · num_inner                           (teardown scaling)
```

where `act_outer` is the running count of outer tuples actually consumed
(summed over every M-block, hence the exact `|R|` once the scan completes) and
`num_inner` is the planner's row estimate for `|S|`, since this version
rescans `S` per M-block rather than materializing it. `est_den` (the
unweighted running pair count `Σ pairs_r`) is tracked for diagnostics only; the
point estimate divides by `weight_sum`, not `est_den`.

**Scaling caveats.** The `act_outer·num_inner` scaling is the teardown
(`ROSL_SUMM`) figure only. The per-round trajectory (`ROSL_TRAJ`) scales by
the planner's `num_outer` instead, since `act_outer` is still growing mid-run.
And `act_outer` is the *scanned* outer — exact for `|R|` only if the outer
scan completed. At a data-dependent stop (output LIMIT, client cut), teardown
rescales to the scanned-`R × S` sub-join, biased low for `J` by roughly
`act_outer/|R|`, while trajectory rows keep the planner scaling. A consumer
that prefers `ROSL_SUMM` over the last trajectory row (the current harness
does) inherits that bias on early-stopped runs and should compare `act_outer`
against `num_outer` before trusting the summary there. This is the
estimand-side companion of the stopping-rule caveat in §2.9.

### 2.4 Sampling distribution: ε-greedy on joinability, fixed

```
q(t) = r(t) / a(t)     if a(t) > 0
q(t) = 1                if a(t) = 0        (untried: optimistically fully joinable)

p(t) = (1 − ε)·q(t)/Q_total + ε/|A|     if Q_total > 0
p(t) = 1/|A|                             if Q_total = 0
```

`Q_total = 0` (every tuple probed, none ever joined) falls back to uniform.
`ε = ROSL_EPSILON` is fixed for the whole run. The ε-floor is retained and is
load-bearing for inference: it guarantees the propensity-decay bound (paper
Eq. 13, with `α = 0`) and keeps every `π(t) > 0`.

### 2.5 Self-normalized variance and the primary CI (O(1) per round)

Written literally, recentering every prior round's residual against the latest
`μ̂` would cost O(rounds) per emission and O(rounds²) over a run. The node
instead expands the square (König–Huygens form) and maintains three scalar
moment accumulators, each updated in O(1):

```
M1 = mom_yy = Σ_r h_r² Ŷ_r²
M2 = mom_yp = Σ_r h_r² Ŷ_r · pairs_r
M3 = mom_pp = Σ_r h_r² pairs_r²

SS = M1 − 2μ̂·M2 + μ̂²·M3
V̂  = SS / weight_sum²
ci_halfwidth = 1.96 · pop · √V̂          (pop = act_outer·num_inner at teardown)
```

This is the primary interval. **Scoping: it is a fixed-horizon guarantee.**
Nominal coverage attaches to the interval reported at the run's natural end
(all blocks processed), not to the running trajectory and not at
data-dependent stopping times. The per-round trajectory CI is a diagnostic.
`V̂` also treats rounds as the independence unit, whereas rounds within an
M-block share adaptive feedback through the accumulated `q` — the block-level
brackets of §2.8 bracket that effect.

### 2.6 Split-cache within-round variance (`v_r`)

`draw_and_dedup_cache` draws the `L = exp_cache_lim` with-replacement indices
once, as before; the only addition is recording, per draw and **before the
sort that destroys draw order**, which of the two contiguous halves
(`[0, L_A)` or `[L_A, L)`, `L_A = ⌊L/2⌋`) it fell in, into `st->in_half_a[]` /
`st->in_half_b[]`. Because the `L` draws are i.i.d., the two halves are
independent with-replacement samples of sizes `L_A` and `L_B = L − L_A`, each
with its own reduced inclusion probability computed **per tuple directly from
the frozen `p(t)`** (not from `pi_repr`):

```
π_A(t) = 1 − (1 − p(t))^{L_A},     π_B(t) = 1 − (1 − p(t))^{L_B}
```

`finalize_round` accumulates `corrA`/`corrB` alongside `corr` in the same loop
over the cache (the shared baseline cancels in the difference of two AIPW
scores, so only the correction needs a per-half version);
`finalize_explore_round` does the same with raw-HT half sums `Y_A`/`Y_B`. Both
then compute

```
v_r = ¼ (corrA − corrB)²   (exploitation)     v_r = ¼ (Y_A − Y_B)²   (exploration)
```

an unbiased estimate of `Var( ½(Ŷ_A + Ŷ_B) | H_{r-1} )`. Two honest caveats,
carried from the struct comments: (a) `v_r` is never a weight — it feeds only
the interval-side accumulators below; and (b) it is a mild **upper** gauge of
the full-cache score's own noise (`π` at `L/2` is smaller than `π` at `L`), so
it errs conservative in the "don't fully trust the clustered CI" direction of
`het_ratio`. The restructured baseline-plus-correction score was verified
identical to the two-loop AIPW form to `1e-12`, so the telemetry is purely
additive.

### 2.7 Block closing (`close_block`)

`close_block` is called at every M-block boundary and once more at teardown.
It folds the currently-open block's partials into two running sums of squares
before clearing them:

```
clust_ss += ( blk_ynum − μ̂·blk_wpair )²
split_ss += ( blk_ya − (blk_wa/blk_wb)·blk_yb )²       (if blk_wa, blk_wb > 0)
```

using the current running `μ̂ = est_num/weight_sum` for the clustered residual
(an O(1/n_blocks) approximation to the fixed-`μ̂` residual that vanishes as
blocks accumulate). `blk_ya/blk_wa` (odd rounds) and `blk_yb/blk_wb` (even
rounds) are the split-half partials, keyed by `blk_round & 1`. `n_blocks`
counts the completed M-blocks folded in.

### 2.8 The interval family (all telemetry / alternative brackets)

At teardown, `PrintRoslCounters` assembles the `ROSL_SUMM` line from these
accumulators with `pop = act_outer · num_inner` (subject to the early-stop
caveat of §2.3). None feeds back into `μ̂`.

- **`ci_halfwidth`** (§2.5) — the primary self-normalized fixed-horizon CI,
  recomputed at teardown against the exact-if-completed `act_outer`.
- **`ci_clust`** — the M-block-clustered interval: the block, not the round,
  is the independence unit. Uses a sparse `t`-table (`t_crit_975`, linear in
  `1/df`, normal limit above `df = 120`) at `df = n_blocks − 1` with a
  `n_blocks/(n_blocks − 1)` small-sample scale on `clust_ss`. It repairs the
  round-level interval's understatement of within-block feedback correlation,
  but **conflates cross-block truth heterogeneity with noise** (since M-blocks
  tile `R`, block-to-block differences in true density are not variance).
  Falls back to `ci_halfwidth` with fewer than two blocks.
- **`ci_split`** — the split-half within-block interval, a heterogeneity-free
  floor built from odd/even round halves at `df = n_blocks`. Between-half
  feedback correlation shrinks it, so it is anti-conservative in exactly the
  clustered CI's target regime. Falls back to `ci_halfwidth` if `split_ss = 0`.
- **`ci_eb` / `est_eb`** — the empirical-Bernstein guard band, a Maurer–Pontil
  **fixed-n** bound with a plug-in range `B = eb_max`, applied to the bounded
  per-round rates `X_r = Ŷ_r/pairs_r`. It needs no variance convergence and no
  regime conditions, making it the most defensible interval at a data-dependent
  stop or a mid-run look. Crucially, the Maurer–Pontil bound is stated for the
  **unweighted** per-round mean `X̄`, so this interval is centered on its **own**
  point estimate `est_eb = X̄ · pop`, emitted alongside — coverage of `ci_eb`
  must be scored against `est_eb`, not the weighted `Ĵ`. Maintained from
  `eb_sx`, `eb_sxx`, `eb_max`, `eb_n`. As implemented it is a robustness guard
  band, not a proven anytime-valid confidence sequence (a fully time-uniform
  version replaces `ln(2/α)` with a stitched iterated-logarithm boundary and is
  slightly wider).
- **`ci_noise`** — a noise-only interval from the split-cache track alone:
  `1.96 · pop · √var_noise / weight_sum`.
- **`het_ratio`** — the decomposition diagnostic,
  `(clust_ss · n_blocks/(n_blocks−1)) / var_noise`. Because `var_noise` is an
  (upper-gauged) noise-only track and `clust_ss` mixes noise with cross-block
  truth heterogeneity, their ratio localizes the regime: `≈ 1` means the
  clustered residuals are mostly estimator noise, so `ci_clust` is honest;
  `≫ 1` means cross-block truth heterogeneity dominates, inflating `ci_clust`'s
  width by roughly `√het_ratio` (the ratio is of variances, not widths). The
  sentinel `−1` marks the undefined case (`var_noise = 0`, or fewer than two
  blocks). Since `var_noise` is upper-gauged, `het_ratio` is if anything biased
  downward — it errs toward reporting the clustered CI as *less* trustworthy
  than it truly is, the safe direction. This is the principled repair of the
  clustered/split bracket: rather than pick a side, the node reports both plus
  the ratio that says which to trust.
- **`lindeberg_max`** — `max_r h_r² v_r`, the largest single round's
  contribution to `var_noise`, certifying that no one round dominates the
  pooled noise (a finite-sample proxy for the Lindeberg condition behind the
  normal approximation).

### 2.9 Known limitations / open points

- The round-level `ci_halfwidth` treats rounds as the independence unit and
  can under-cover when within-block feedback is strong; that is precisely what
  `ci_clust` and `het_ratio` exist to expose. Read them together. Neither
  block-level interval should displace the round-level `ci_halfwidth` until the
  bracket is re-run on the real TPC-H z1 arrays.
- **An output-LIMIT stop biases the point estimate itself**, not just the
  interval. It is a stopping rule correlated with the estimand (it halts when
  the N-th match is emitted), so *every* emitted interval — `ci_eb` included —
  fails to cover a LIMIT-stopped run. Anytime-validity fixes the interval, not
  a corrupted center; a stop-aware point correction is an open problem. Run
  coverage-validation sweeps for `ci_halfwidth` without output limits.
- The AIPW control variate `q̂(t)` predicts *average* joinability, not this
  round's specific match count against `K`; if match density varies a lot
  across K-blocks it helps less, though empirically the predictor's *form*
  (`q̂ = 0` for untried) matters far more than its per-K accuracy.
- The finite-population / heterogeneous-slice regime differs from the paper's
  fixed-arm i.i.d.-outcome setting; tuples are assumed to be in random order
  (datasets shuffled before testing), with empirical coverage as the practical
  gate. Confirmation on the TPC-H z1 schemas is pending.

### 2.10 Relationship to the legacy path (`ROSL_FLAT_WEIGHTS = 0`)

The flat/legacy switch changes exactly one thing — how `h_r` is computed — and
everything downstream of `accumulate_round` is written against `h_r`
generically, so no other function branches on it.

- **Not compiled on the flat path:** `two_point_lambda`, and inside
  `accumulate_round` the stick-breaking weight computation (with its underflow
  guard) and the `V_r = pairs_r/π_repr` variance proxy's role as a weight.
- **Dead state retained** (so the A/B rebuild changes no struct layout or
  signature): the `ROSL_ALPHA` define, the `st->alpha` and `st->stick` fields
  with their unconditional init and rescan reset, and the `pi_repr` argument
  threaded through `accumulate_round` / the two finalize functions. On the flat
  path `pi_repr` enters neither `h_r` nor the split-cache halves (those use
  `p(t)` directly), so it is pure dead weight.
- **Unchanged across both paths:** the AIPW score, the König–Huygens moment
  accumulators, the split-cache halves and `v_r`, the block-clustered /
  split-half machinery, and the empirical-Bernstein band.

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
runs at stock-nested-loop speed with `enable_rosl = on`.

### Tunable constants

Defined at the top of the file:

| Constant             | Value     | Meaning                                                            |
|----------------------|-----------|-------------------------------------------------------------------|
| `ROSL_M_LIM`         | 1588      | Outer (R) tuples per M-block                                       |
| `ROSL_K_LIM`         | 1588      | Inner (body) tuples per exploitation K-block                       |
| `ROSL_EXP_CACHE_LIM` | 529       | With-replacement draws per round, either phase (`L`)              |
| `ROSL_N_PROBES`      | 1588      | Size of the exploration prefix `P` (redrawn every M-block)         |
| `ROSL_EPSILON`       | 0.2       | Fixed exploitation exploration mass (`ε`; no decay — §2.4)         |
| `ROSL_EPSILON_FLOOR` | 0.2       | Propensity-decay floor (precondition for valid inference)          |
| `ROSL_FLAT_WEIGHTS`  | 1         | 1 = flat pooling `h = 1/pairs` (default); 0 = legacy stick-breaking |
| `ROSL_ALPHA`         | 0.7       | Two-point allocation decay exponent (`α`) — **legacy path only**   |
| `ROSL_TRAJ_CAP`      | 1,000,000 | Max per-round trajectory rows retained for the dump                |

`ROSL_ALPHA` is read only when `ROSL_FLAT_WEIGHTS = 0`; under the default flat
pooling it is dead state (§2.10). `ROSL_EPSILON_FLOOR` is retained as the
propensity floor that keeps every `π(t) > 0`.

### Cost per round

The point estimate `Ĵ` is O(1) per round given the running accumulators. The
variance is the only part that could go super-linear, and the König–Huygens
moment form keeps it O(1) per emission. The split-cache track adds two
half-cache corrections over the same cache the score already scans (one extra
`pow` per cached tuple per half) plus two accumulator updates; the clustered /
split-half brackets and the empirical-Bernstein band are each a handful of
O(1) accumulators. Net: the full estimate-plus-intervals update is O(1) per
round beyond the one O(|M|) distribution-build pass, dominated by the sampling
cost around it (distribution build O(|M|), cache draw O(L·log L), probing
O(|cache|·|K|)) — so the node emits every round.

### State machine

A single `ExecNestLoop` call advances a small phase machine and yields at
most one row per call (resume cursors let a round span many calls):

- **`PH_NEW_MBLOCK`** — load the next outer block; if `R` is exhausted, go to
  `PH_DONE`. Close the previous block's partials (`close_block`), zero
  `reward[]`/`attempts[]` for the new block, rescan `S`, and load its first
  `n_probes` tuples into the exploration prefix `P`. On the first non-empty
  block the per-round timing clock starts; every block adds to `act_outer`.
- **`PH_EXPLORE`** — build a *uniform* distribution over the M-block, draw and
  dedup the exploration cache (tagged into split halves A/B before the sort),
  and probe it against all of `P`, emitting matches one per call (resume
  cursors `explore_ci` / `explore_pj`). Skipped to `PH_NEW_SBLOCK` if `P` came
  back empty. On completion, `finalize_explore_round` folds the raw-HT score
  and `v_r`.
- **`PH_NEW_SBLOCK`** — load the next body K-block; if the body is exhausted,
  advance to `PH_NEW_MBLOCK`. Build the joinability-weighted, fixed-`ε`
  distribution and freeze `q(t)` / `q̂(t)` (`build_distribution`), draw and
  dedup the exploit cache (split-tagged), and reset per-round match counts.
- **`PH_PROBE`** — probe the exploit cache against the K-block, emitting
  matches one per call (resume cursors `probe_ci` / `probe_kj`). When the
  round's last pair is probed, `finalize_round` folds the AIPW score, the
  moment / noise / EB accumulators, and the block partials, and records the
  trajectory row (with `ci_halfwidth` and `ci_eb`), then returns to
  `PH_NEW_SBLOCK`.
- **`PH_DONE`** — outer relation exhausted; the cursor returns NULL.

Completion is signalled to the client purely by the cursor returning NULL —
no mid-stream message is emitted (see §4).

### Correctness notes

- **Per-state RNG.** Cache draws use an `xorshift64*` stream seeded per
  `RoslJoinState` from a mix of high-resolution time, the backend PID, and the
  state pointer — not a process-global `rand()`/`srand()`. A shared,
  second-resolution seed would hand identically-timed concurrent benchmark
  workers identical "random" caches, correlating runs meant to be independent
  replicates and invalidating exactly the cross-run variance the confidence
  intervals report.
- **Plan-shape guard.** Falling back to the stock join rather than sampling
  under an assumption that doesn't hold trades a slower query for a correct
  one.
- **Exact outer population.** The final summary uses `act_outer` — the exact
  sum of every M-block's size — for `|R|`; `|S|` stays the planner's
  `num_inner`, because this version rescans `S` per M-block instead of
  materializing it once and so never counts it exactly. The running trajectory
  scales by the planner's `num_outer` for both, since `act_outer` is still
  growing mid-run (§2.3).

---

## 4. Output protocol — server-log only

ROSL does **not** stream estimates back to the client mid-query. It
accumulates the entire per-round trajectory in its executor state and dumps it
**once, at executor teardown** (`ExecEndNestLoop` → `PrintRoslCounters`), as
`elog(INFO, ...)` lines to the **PostgreSQL server log**. Row delivery and
measurement delivery are on separate channels.

This decoupling is deliberate: an earlier design emitted a per-round `NOTICE`
during row production, which deadlocked a server-side cursor because notices
are only flushed at fetch boundaries. With the log-only dump, nothing the
client must parse is interleaved with rows, so that class of hang is
structurally impossible.

### Log line formats

Per-round trajectory (one line per round — exploration and exploitation rounds
both recorded, in order):

```
ROSL_TRAJ round=<n> mean_per_pair=<μ̂> est_join=<Ĵ> ci_halfwidth=<half> pairs_seen=<est_den> sample_matches=<m> elapsed_ms=<t> ci_eb=<eb-half>
```

`ci_halfwidth` is the running self-normalized diagnostic CI (§2.5); `ci_eb` is
the empirical-Bernstein guard-band half-width (§2.8). Note the trajectory
carries `ci_eb` as a running **width indicator only** — there is no per-round
`est_eb`, so it is not a coverage-scorable interval mid-run; `est_eb` is
emitted only at teardown. Trajectory `Ĵ` and both widths use the planner's
`num_outer · num_inner` scaling.

Final summary (one line at teardown):

```
ROSL_SUMM final_est_join=<Ĵ> ci_halfwidth=<half> ci_clust=<half> ci_split=<half> ci_eb=<half> est_eb=<Ĵ_eb> ci_noise=<half> het_ratio=<r> lindeberg_max=<x> n_blocks=<G> rounds=<n> sample_matches=<m> pairs_seen=<est_den> t_steps=<t> act_outer=<|R| exact> num_outer=<|R| planner est.> num_inner=<|S| planner est.>
```

`final_est_join` and `ci_halfwidth` use the exact `act_outer` for `|R|` rather
than the planner's `num_outer` (§2.3); both are logged so they can be
compared. The interval family (§2.8): `ci_clust` (M-block-clustered),
`ci_split` (split-half within-block), `ci_eb`/`est_eb` (empirical-Bernstein
band and **its own** centering estimate — score `ci_eb` coverage against
`est_eb`, every other interval against `Ĵ`), `ci_noise` (noise-only),
`het_ratio` (heterogeneity diagnostic; `−1` = undefined), `lindeberg_max`
(Lindeberg telemetry), `n_blocks` (the clustering `G`).

If the outer relation was empty or no round ever completed:

```
ROSL_SUMM final_est_join=0.00 rounds=0 sample_matches=<m> (no completed rounds: outer empty or join produced no pairs)
```

If a run exceeds `ROSL_TRAJ_CAP` rounds, the first `ROSL_TRAJ_CAP` are kept
and a `ROSL_TRAJ truncated: ...` line is emitted (use a smaller scale factor).
Per-round `elapsed_ms` is measured in C at the source (relative to the first
M-block load), independent of when the client fetches rows.

### Cluster requirements

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

ROSL must run as a plain, fixed-inner nested loop — the shape the guard in §3
requires to actually engage rather than silently reverting to the stock join.
The benchmark worker sets (among others):

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

> **Coverage caveat.** Runs that hit these LIMITs stop on a rule correlated
> with the estimand and bias the point estimate low (§2.9); coverage-validation
> sweeps for `ci_halfwidth` should run without output limits.

### Trajectory CSV columns

```
size, query, zval, shuffle, repeat, round, pairs_seen, sample_matches,
mean_per_pair, est_join, truth, ratio, rel_error, elapsed_sec, delta_sec,
pct_of_truth_output
```

`ratio = est_join / truth` and `rel_error = (est_join − truth)/truth` are the
accuracy columns; `pct_of_truth_output` is the natural x-axis for
accuracy-vs-progress charts.

> **Note:** the per-round log line now carries `ci_halfwidth` and `ci_eb`, and
> the summary line carries the full interval family (`ci_clust`, `ci_split`,
> `ci_eb`/`est_eb`, `ci_noise`, `het_ratio`, `lindeberg_max`, `n_blocks`). If
> `worker.py`'s log parser wasn't updated alongside the estimator, add these
> fields (and, if useful, derived `ci_lower`/`ci_upper`) to the merged schema
> so interval coverage can be checked against `truth` — remembering to score
> `ci_eb` against `est_eb`, not `est_join`. Since the schema is additive, older
> per-job CSVs won't merge with new runs — the manager's header-mismatch guard
> fails loudly, so give each sweep a fresh results directory.

---

## 6. File map

| File                  | Role                                                                                              |
|-----------------------|---------------------------------------------------------------------------------------------------|
| `nodeNestloop.c`      | ROSL executor node (two-phase sampling join + flat-weighted AIPW estimator + interval family + log dump) |
| `tpch_manager.py`     | Sweep manager: fan-out, timing, CSV merge                                                          |
| `worker.py`           | Per-config runner: truth, ROSL run, server-log trajectory read                                    |
| `accuracy_charts.py`  | Accuracy plots from merged `trajectory.csv`                                                        |