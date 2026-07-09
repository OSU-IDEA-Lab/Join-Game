# ROSL — Flat-Weighted AIPW with Anytime-Valid Confidence Sequence and Last-Safe-Point Truncation Scaling

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
Section 9 of the design notes: the Section-8 flat-weighted AIPW estimator
(Section 7's adaptively-weighted estimator with flat pooling weights and the
split-cache within-round variance track folded in, the legacy stick-breaking
branch left compiled out) with two further changes on top:

- the empirical-Bernstein guard band is replaced by **Howard et al.'s stitched
  time-uniform empirical-Bernstein confidence sequence** — a *proven*
  anytime-valid interval under the `ROSL_CS_CMAX` range assumption — with the
  old fixed-n Maurer–Pontil band retained behind `ROSL_KEEP_MP_BAND` (token
  `ci_mp`) for one A/B release; and
- the teardown summary uses **last-safe-point population scaling** on
  truncated runs: an early-stopped run (output LIMIT, client cut) is scaled by
  the planner's `num_outer` instead of the scanned `act_outer`, making
  `final_est_join` equal the last trajectory row rather than a biased-low
  scanned-sub-join figure, with a `run_complete=0/1` token classifying every
  run.

The sampling procedure is the
probabilistic-N-failure / ε-greedy-on-joinability procedure of Section 6,
unchanged; only the estimator-side machinery differs.

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

pop_outer = outer_exhausted ? act_outer : num_outer      (teardown)
Ĵ = μ̂ · pop_outer · num_inner
```

where `act_outer` is the running count of outer tuples actually consumed
(summed over every M-block, hence the exact `|R|` once the scan completes),
`outer_exhausted` is a flag set only at the single site where
`load_outer_block` runs dry (never inferred from the terminal phase), and
`num_inner` is the planner's row estimate for `|S|`, since this version
rescans `S` per M-block rather than materializing it. `est_den` (the
unweighted running pair count `Σ pairs_r`) is tracked for diagnostics only; the
point estimate divides by `weight_sum`, not `est_den`.

**Population-scale contract (last safe point).** On a *complete* run
(`outer_exhausted`, logged as `run_complete=1`), teardown scales by the exact
scanned `act_outer`; output is byte-identical to pre-Section-9 builds except
the trailing token. On a *truncated* run (output LIMIT, client cut;
`run_complete=0`), teardown falls back to the planner scale
`μ̂ · num_outer · num_inner` — equal by construction to the last emitted
`ROSL_TRAJ` row (the last completed round, i.e. the last safe point), unless
the trajectory buffer hit `ROSL_TRAJ_CAP`, in which case the log's last row
predates it. The old behaviour — scaling a truncated run by the scanned
`act_outer` — rescaled to the scanned-`R × S` sub-join, biased low for `J` by
roughly `act_outer/|R|`, and is deliberately removed; do not reintroduce it.
Truncated-run accuracy is instead bounded by the quality of the planner's
`num_outer` estimate — the exact-`|R|` correction exists only on complete
runs. `act_outer` is still printed raw so consumers can recover the seen
fraction. Partial in-flight rounds are discarded at teardown, never salvaged —
that discard is what makes the last-TRAJ-row equality hold. This is the
estimand-side companion of the stopping-rule caveat in §2.9: the scale switch
removes the *rescaling* bias, not the stopping-rule bias in `μ̂` itself.

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
ci_halfwidth = 1.96 · pop · √V̂          (pop = pop_outer·num_inner at teardown, §2.3)
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
accumulators with `pop = pop_outer · num_inner` (the conditional last-safe-point
scale of §2.3). None feeds back into `μ̂`.

- **`ci_halfwidth`** (§2.5) — the primary self-normalized fixed-horizon CI,
  recomputed at teardown against `pop_outer` (exact `act_outer` on complete
  runs, planner `num_outer` on truncated ones).
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
- **`ci_eb` / `est_eb`** — the anytime-valid guard band: Howard et al.'s
  **stitched time-uniform empirical-Bernstein confidence sequence** on the
  bounded per-round rates `X_r = Ŷ_r/pairs_r`, replacing the old fixed-n
  Maurer–Pontil form. Under the predictable analytic range envelope
  (`ROSL_CS_ANALYTIC_B = 1`, which assumes `ROSL_CS_CMAX` bounds the per-pair
  match multiplicity — equijoin-on-key only), `ci_eb` is a **proven
  time-uniform sequence**, valid at arbitrary look times, with two-sided
  miscoverage `ROSL_CS_ALPHA` over *all* look times; with
  `ROSL_CS_ANALYTIC_B = 0` the plug-in realized range (`eb_max`) makes it a
  robustness heuristic instead (the old caveat, now narrowed to that path).
  The boundary is computed by `cs_boundary` from the intrinsic-time variance
  process `cs_v` (floored at `ROSL_CS_V_MIN`) with stitching parameters
  `ROSL_CS_ETA` / `ROSL_CS_S`; it is defined — and honestly huge — from round
  1, fixing the old zero-width start. As before, the bound is stated for the
  **unweighted** per-round mean `X̄`, so the interval is centered on its **own**
  estimate `est_eb = X̄ · pop` (identical to the pooled `μ̂` under the default
  flat weights, differing only on the legacy weighted path) — coverage of
  `ci_eb` must be scored against `est_eb`, not the weighted `Ĵ`. The CS makes
  the interval honest at every look time; it is interval-valid, **not
  center-corrective** (§2.9).
- **`ci_mp`** — the legacy fixed-n Maurer–Pontil band, kept behind
  `ROSL_KEEP_MP_BAND` for one A/B release only. `0.00` when compiled out or
  with fewer than two rounds.
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

**At a glance.** The intervals differ along two axes that are otherwise spread
across §2.5, §2.8, and §2.9 — what kind of validity each has, and which point
estimate each brackets:

| Interval | Validity type | Centered on | Valid at a data-dependent stop? |
|----------|---------------|-------------|---------------------------------|
| `ci_halfwidth` | fixed-horizon (self-normalized) | `Ĵ` | No |
| `ci_clust` | fixed-horizon (block-clustered) | `Ĵ` | No |
| `ci_split` | fixed-horizon (split-half floor) | `Ĵ` | No |
| `ci_noise` | fixed-horizon (noise-only) | `Ĵ` | No |
| `ci_eb` (CS) | **anytime-valid** (proven under `ROSL_CS_ANALYTIC_B = 1`) | `est_eb` | **Yes — for its prefix target** |
| `ci_mp` | fixed-`n` (legacy Maurer–Pontil, A/B only) | `est_eb` | No |

`het_ratio` and `lindeberg_max` are diagnostics, not intervals. Two things the
table compresses: (a) `ci_eb` is the **only** interval valid at a
data-dependent stop, and only for **its own** target — the prefix mean `est_eb`
brackets (the running average slice rate over rounds seen so far), which equals
full-`J` only insofar as the prefix is representative (§2.9); under
`ROSL_CS_ANALYTIC_B = 0` even that degrades to a heuristic. (b) `ci_eb`/`ci_mp`
center on `est_eb`; everything else centers on `Ĵ` — score coverage
accordingly. Empirical coverage on the TPC-H z1 schemas is the practical gate
and is still pending (§2.9), so these validity labels are the intervals'
*designed* guarantees, not yet measured coverage.

### 2.9 Known limitations / open points

- The round-level `ci_halfwidth` treats rounds as the independence unit and
  can under-cover when within-block feedback is strong; that is precisely what
  `ci_clust` and `het_ratio` exist to expose. Read them together. Neither
  block-level interval should displace the round-level `ci_halfwidth` until the
  bracket is re-run on the real TPC-H z1 arrays.
- **An output-LIMIT stop still biases the point estimate itself.** It is a
  stopping rule correlated with the estimand (it halts when the N-th match is
  emitted). The Section-9 changes address the two *repairable* pieces: the
  last-safe-point scale (§2.3) removes the additional rescaling bias the old
  `act_outer` teardown added, and the stitched CS makes `ci_eb` time-uniformly
  **valid** at any look time — but for the prefix target (the average slice
  rate over rounds seen so far, per the Section-7 note: a LIMIT stop biases
  what that prefix represents relative to full-`J`). Anytime-validity fixes
  the interval, not a corrupted center; an escape late in a capped run
  reflects the known center bias, not a CS failure. A stop-aware point
  correction remains an open problem. Run coverage-validation sweeps for
  `ci_halfwidth` without output limits, and use `run_complete=0` to segregate
  truncated cells in analysis.
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
| `ROSL_CS_ALPHA`      | 0.05      | CS two-sided miscoverage over **all** look times                   |
| `ROSL_CS_ETA`        | 2.0       | Stitching geometric spacing (`η > 1`)                              |
| `ROSL_CS_S`          | 1.4       | Stitching polynomial exponent (`s > 1`; `ζ(s)` is hard-coded — update the constant in `cs_boundary` if changed) |
| `ROSL_CS_V_MIN`      | 1e-12     | Variance-process floor for the boundary                            |
| `ROSL_CS_ANALYTIC_B` | 1         | 1 = predictable analytic range envelope (proven CS); 0 = plug-in realized range (heuristic) |
| `ROSL_CS_CMAX`       | 1.0       | Per-pair match multiplicity bound assumed by the analytic envelope (equijoin-on-key; raise it or set `ROSL_CS_ANALYTIC_B = 0` for non-key joins) |
| `ROSL_KEEP_MP_BAND`  | 1         | Also emit the legacy fixed-n Maurer–Pontil band (`ci_mp`) — one A/B release |
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
- **Exact outer population — on complete runs.** The final summary uses
  `act_outer` — the exact sum of every M-block's size — for `|R|` only when
  `outer_exhausted` certifies the scan ran dry; truncated runs fall back to
  the planner's `num_outer` (the last safe point, §2.3). `|S|` stays the
  planner's `num_inner`, because this version rescans `S` per M-block instead
  of materializing it once and so never counts it exactly. The running
  trajectory scales by the planner's `num_outer` for both, since `act_outer`
  is still growing mid-run. The `outer_exhausted` flag is initialized and
  reset alongside `act_outer` (including on `ExecReScan`) and set only at the
  single `load_outer_block`-runs-dry transition.

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
ROSL_TRAJ round=<n> mean_per_pair=<μ̂> est_join=<Ĵ> ci_halfwidth=<half> pairs_seen=<est_den> sample_matches=<m> elapsed_ms=<t> ci_eb=<cs-half> est_eb=<Ĵ_eb> ci_mp=<mp-half>
```

`ci_halfwidth` is the running self-normalized diagnostic CI (§2.5); `ci_eb` is
the stitched time-uniform CS half-width (§2.8), and — unlike the old build —
each row also carries `est_eb`, so the trajectory is now a
**coverage-scorable anytime-valid interval at every round**, including a run
stopped by an output LIMIT (score it against `est_eb`; `ci_halfwidth` remains
fixed-horizon-only). `ci_mp` is the legacy Maurer–Pontil width (A/B only,
`0.00` when unavailable). Trajectory `Ĵ`, `est_eb` and all widths use the
planner's `num_outer · num_inner` scaling.

Final summary (one line at teardown):

```
ROSL_SUMM final_est_join=<Ĵ> ci_halfwidth=<half> ci_clust=<half> ci_split=<half> ci_eb=<half> est_eb=<Ĵ_eb> ci_noise=<half> het_ratio=<r> lindeberg_max=<x> n_blocks=<G> rounds=<n> sample_matches=<m> pairs_seen=<est_den> t_steps=<t> act_outer=<|R| scanned> num_outer=<|R| planner est.> num_inner=<|S| planner est.> ci_mp=<mp-half> cs_b=<range env.> cs_v=<intrinsic time> run_complete=<0|1>
```

`final_est_join` and every interval use the conditional last-safe-point scale
(§2.3): exact `act_outer` when `run_complete=1`, planner `num_outer` when
`run_complete=0` — in which case `final_est_join` equals the last `ROSL_TRAJ`
row (unless `ROSL_TRAJ_CAP` was hit). `act_outer` and `num_outer` are both
logged raw so the seen fraction is always recoverable. The interval family
(§2.8): `ci_clust` (M-block-clustered),
`ci_split` (split-half within-block), `ci_eb`/`est_eb` (stitched CS
band and **its own** centering estimate — score `ci_eb` coverage against
`est_eb`, every other interval against `Ĵ`), `ci_noise` (noise-only),
`het_ratio` (heterogeneity diagnostic; `−1` = undefined), `lindeberg_max`
(Lindeberg telemetry), `n_blocks` (the clustering `G`), `ci_mp` (legacy MP
band, A/B only), `cs_b`/`cs_v` (the CS range envelope and intrinsic time, for
offline boundary recomputation). The `cs_b` token carries the **predictable
running-max envelope `cs_bmax`** — the value the boundary actually uses on the
default `ROSL_CS_ANALYTIC_B = 1` path — not the single-round `cs_b`; to
reproduce the half-width offline, feed `cs_boundary` this value as `b`, `cs_v`
(floored at `ROSL_CS_V_MIN`) as `v`, the round count as `n`, and the **per-side**
level `ROSL_CS_ALPHA / 2`. (On the `ROSL_CS_ANALYTIC_B = 0` heuristic path the
boundary instead uses the realized `eb_max`, which is not emitted, so the
summary alone cannot reproduce that band.) `run_complete` is the **last**
key=value token on every summary path, so existing positional eyeballing of old
logs is undisturbed, and it parses with the harness's existing `key=value`
regex — no worker change needed.

If the outer relation was empty or no round ever completed:

```
ROSL_SUMM final_est_join=0.00 rounds=0 sample_matches=<m> run_complete=<0|1> (no completed rounds: outer empty or join produced no pairs)
```

The token appears on this degenerate path too — a LIMIT firing before any
round completes lands exactly here, and without it the most-truncated case
would be the one you can't classify. (A genuinely empty outer yields
`run_complete=1` on this line; a pre-first-round cut yields `0`.)

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

> **Note:** the per-round log line now carries `ci_halfwidth`, `ci_eb`,
> `est_eb`, and `ci_mp`, and the summary line carries the full interval family
> (`ci_clust`, `ci_split`, `ci_eb`/`est_eb`, `ci_noise`, `het_ratio`,
> `lindeberg_max`, `n_blocks`, `ci_mp`, `cs_b`, `cs_v`) plus `run_complete`. If
> `worker.py`'s log parser wasn't updated alongside the estimator, add these
> fields (and, if useful, derived `ci_lower`/`ci_upper`) to the merged schema
> so interval coverage can be checked against `truth` — remembering to score
> `ci_eb` against `est_eb`, not `est_join`, and to segregate `run_complete=0`
> cells (their `final_est_join` is the last-safe-point planner-scaled figure,
> equal to the last trajectory row unless `ROSL_TRAJ_CAP` was hit). Since the
> schema is additive, older
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