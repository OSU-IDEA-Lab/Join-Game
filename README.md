# ROSL — Single-M AIPW with Per-K-Block Cache Redraw and Hadad Weighting

A sampling-based join operator for PostgreSQL that replaces the inner loop of
the executor's nested-loop node with a joinability-guided (bandit-style)
sampler and produces a running, confidence-interval-backed estimate of the
total join size as it streams matching rows. ROSL trades exactness for early,
progressively-accurate estimates: it can report an approximate `|R ⋈ S|` — with
an error bar — long before a full join would finish.

The node ships **two mutually-exclusive estimator drivers**, selected at
compile time:

- **Classic sequential tiling** (`ROSL_SINGLE_M = 0`, the default) — consumes
  `R` in M-blocks, rescans `S` per block with a two-phase
  exploration/exploitation split, and pools every round with **flat weights**.
  This is the shipped default and is byte-for-byte the estimator described in
  the flat-weighted design notes; when `ROSL_SINGLE_M = 0` every single-M symbol
  is `#if`'d out.
- **Single-M** (`ROSL_SINGLE_M = 1`) — draws **one M-block once**, uniformly,
  and holds it for the entire run while streaming `S` in K-blocks with **one
  fresh cache realization per K-block round**. This is the configuration in
  which the Hadad et al. (2021) adaptively-weighted AIPW machinery applies
  essentially verbatim, so this mode restores the paper's variance-stabilizing
  pooling weights (`ROSL_WEIGHT_MODE`) behind an I3-legal predicted-variance
  correction, and adds a **two-stage confidence interval** that prices the
  M-lottery sampling error explicitly.

This document describes the estimator **exactly as it runs in
`nodeNestloop.c`**, tied to the struct fields and functions that implement it.
Sections 1–4 cover behaviour common to both drivers and the classic tiling
driver in detail; Section 5 covers the single-M driver. The sampling procedure
(probabilistic-N-failure / ε-greedy-on-joinability) is unchanged across both;
only the block protocol, the pooling weights, and the interval family differ.

## What single-M is, and why it exists

The classic tiling driver tiles `R` into M-blocks that have **genuinely
different per-block truths**, which is the reason its pooling weights were
forced flat: inverse-variance weighting targets a precision-weighted mean that
equals the population mean only under a common conditional mean across rounds,
so variance-tracking weights bias the pooled estimand toward low-variance
slices. The governing rule there is **variance information belongs in the
intervals, never in the weights** — and the paper's stick-breaking weights
survive only as a compiled-out A/B path.

The single-M driver removes exactly that obstruction. With **one M-block held
fixed for the whole run**, and `S` streamed in K-blocks with a fresh cache each
round, the arm set is fixed and finite conditional on `M`, the potential
outcomes are i.i.d. across rounds (i.i.d.-tuples assumption plus the physical
shuffle), the estimand is a single common conditional mean `p_M` for every
round, the ε-floored cache supplies known history-adapted propensities with
`e_t(r) ≥ ε/m`, and the horizon `T` is known up front. Under these conditions
the Hadad adaptively-weighted machinery is legal, and this mode turns it back
on.

Two consequences are **accepted and priced, never hidden**:

- The estimand conditional on `M` is `J_M = (N_R/m)·Σ_{r∈M} deg_S(r)`, not the
  realized join size `J`. The gap `J_M − J` is a one-shot finite-population
  sampling error, reported as a **second CI stage** (`ci_between`).
- Emitted join rows come only from `M × S`, so this is an **estimator node**,
  not a full-join replacement. `n_rows` and any output-fraction column change
  meaning relative to the tiling mode; the summary carries `m_size` so no
  cross-mode row is ambiguous.

**The one correction that makes single-M sound** (vs the retired −34%-bias
adaptive-weights build): the variance proxy entering the stick-breaking
recursion is the **pre-round predicted variance `V_pred_t` computed from the
frozen sampling distribution `q_t`** (which is `H_{t-1}`-measurable), never the
current round's realized variance. That single substitution — invariant I3
below — is what separates a biased estimator from a theoretically sound one.

---

## 1. What ROSL does (common core)

Given outer relation `R` and inner relation `S`, both drivers:

1. Build a per-M-block sampling distribution over the M-block's tuples,
   optionally weighted by accumulated **joinability** (each tuple's cumulative
   match count divided by its cumulative probe count; untried tuples are
   optimistically full-joinability `q(t) = 1`), blended with a **fixed**
   exploration mass `ε`.
2. Draw `L = exp_cache_lim` tuples **with replacement** from that distribution,
   deduplicate them into a cache (tagging each draw into split-halves A/B before
   the sort that destroys draw order), and probe that cache against a block of
   `S`, emitting matches.
3. Score each round with an **AIPW** (augmented inverse-probability-weighted)
   estimate, using a **frozen** per-tuple match-rate predictor `q̂(t)` as the
   control variate, and fold it into a running estimate `Ĵ` plus a family of
   confidence intervals, all in O(1) per round.

The fixed `ε` is load-bearing for inference in both drivers: it bounds every
selection probability below by `ε/|A|`, keeping every inclusion probability
`π(t) > 0` so the AIPW correction and both half-cache corrections are always
well-defined (paper Eq. 13 with `α = 0`).

The AIPW predictor `q̂(t)` and the selection value `q(t)` deliberately diverge
for untried tuples — selection stays optimistic (`q = 1`) to preserve
exploration bandwidth; the score predicts `q̂ = 0`. The optimism must never leak
into the score; both are frozen into `st->q[t]` / `st->qhat[t]` **before** the
round's cache is drawn, so they are `H_{r-1}`-measurable:

```
q̂(t) = r(t) / a(t)     if a(t) > 0     (frozen match-rate predictor)
q̂(t) = 0                if a(t) = 0     (untried: predict ZERO matches)
```

The AIPW score (both drivers), in baseline-plus-correction form so the baseline
is accumulated once over `M` and only the residual correction scans the small
cache:

```
Ŷ_r = Σ_{t ∈ M}  q̂(t)·|K|                          (base, over all of M)
    + Σ_{t ∈ C}  ( match(t) − q̂(t)·|K| ) / π(t)     (corr, over the cache)

π(t) = 1 − (1 − p(t))^L                             (inclusion probability)
```

Because most tuple pairs don't join, `q̂(t) ≈ 0` for most tuples, the baseline
stays small, and the `1/π` correction fires only on the rare tuples that
actually match — which is where most of the estimator's variance under sparse
joins used to come from.

### Split-cache within-round variance (`v_r`)

The `L` with-replacement draws are split into two contiguous halves before
dedup (`[0, L_A)` / `[L_A, L)`, `L_A = ⌊L/2⌋`), recorded per draw into
`st->in_half_a[]` / `st->in_half_b[]`. Because the draws are i.i.d., the halves
are independent with-replacement samples with their own reduced inclusion
probabilities computed **per tuple directly from the frozen `p(t)`**:

```
π_A(t) = 1 − (1 − p(t))^{L_A},     π_B(t) = 1 − (1 − p(t))^{L_B}

v_r = ¼ (corrA − corrB)²     (base cancels in the difference of two AIPW scores)
```

`v_r` is an unbiased estimate of the within-round score noise. It is **never a
weight** — it feeds only the interval-side accumulators — and it is a mild
**upper** gauge of the full-cache score's own noise, so it errs conservative.

---

## 2. The classic tiling estimator (`ROSL_SINGLE_M = 0`, default)

The default driver is unchanged from the flat-weighted build. In brief:

- `R` is consumed in **M-blocks** of `m_lim`; for each, `S` is rescanned, its
  first `n_probes` tuples become a fresh **exploration prefix** `P`, and the
  rest is the **exploitation body** consumed in **K-blocks** of `k_lim`.
- One **exploration round** per M-block scores `M × P` with raw
  Horvitz–Thompson (no control variate yet); each **exploitation round** over a
  K-block scores `M × K` with the AIPW form above.
- Every round is pooled with **flat per-pair weights** `h_r = 1/pairs_r`, so
  `Ĵ` is the plain per-pair average of round scores scaled by
  `act_outer · num_inner`.
- A self-normalized König–Huygens variance backs the primary `ci_halfwidth`,
  and a family of alternative brackets (`ci_clust`, `ci_split`, `ci_eb`/
  `est_eb`, `ci_noise`, `het_ratio`, `lindeberg_max`) is maintained alongside,
  all O(1) per round. None feeds back into the point estimate.

Because M-blocks tile `R`, the block-clustered interval `ci_clust` **conflates
cross-block truth heterogeneity with noise**; `het_ratio` = clustered SS /
noise variance localizes the regime (`≈ 1` clustered CI honest; `≫ 1`
cross-block heterogeneity dominates). This is the driver's known-limitation
story, and it is exactly the obstruction single-M was built to remove.

---

## 3. Output protocol — server-log only

ROSL does **not** stream estimates back to the client mid-query. It accumulates
the entire per-round trajectory in executor state and dumps it **once, at
executor teardown** (`ExecEndNestLoop → PrintRoslCounters`), as `elog(INFO, …)`
lines to the **PostgreSQL server log**. Row delivery and measurement delivery
are on separate channels. (An earlier design emitted per-round `NOTICE`s during
row production and deadlocked a server-side cursor, because notices flush only
at fetch boundaries; the log-only dump makes that class of hang structurally
impossible.)

### Log line formats

**Per-round trajectory** (one line per round, in order):

```
ROSL_TRAJ round=<n> mean_per_pair=<μ̂> est_join=<Ĵ> ci_halfwidth=<half> pairs_seen=<est_den> sample_matches=<m> elapsed_ms=<t> ci_eb=<eb-half>
```

`ci_halfwidth` is the running self-normalized diagnostic CI; `ci_eb` is the
empirical-Bernstein guard-band half-width (running width indicator only — not a
coverage-scorable interval mid-run). Trajectory `Ĵ` and both widths use the
planner's `num_outer · num_inner` scaling.

**Final summary** (one line at teardown; classic driver):

```
ROSL_SUMM final_est_join=<Ĵ> ci_halfwidth=<half> ci_clust=<half> ci_split=<half> ci_eb=<half> est_eb=<Ĵ_eb> ci_noise=<half> het_ratio=<r> lindeberg_max=<x> n_blocks=<G> rounds=<n> sample_matches=<m> pairs_seen=<est_den> t_steps=<t> act_outer=<|R| exact> num_outer=<|R| planner> num_inner=<|S| planner>
```

**Single-M** appends its own numeric tokens to the same line (§5.4) and prints
a free-text mode banner:

```
ROSL_MODE single_m weight=<FLAT|HADAD_CONST|HADAD_2PT> steering=<0|1> m=<m> T_planned=<T>
```

The banner is deliberately **not** parseable by the key=value tokenizer (the
string weight-mode name fails the numeric value class); the machine-readable
`weight_mode` integer lives on the `ROSL_SUMM` line.

If the outer relation was empty or no round completed:

```
ROSL_SUMM final_est_join=0.00 rounds=0 sample_matches=<m> (no completed rounds: outer empty or join produced no pairs)
```

### Cluster requirements

```
logging_collector = on        # a managed logfile exists on disk (restart-only GUC)
log_min_messages  = info      # INFO reaches the log (worker sets per session)
log_line_prefix   includes %p # per-backend PID, so concurrent workers can be told apart
```

For the cleanest behaviour during a large sweep, pin a single logfile with
`log_rotation_size = 0` and `log_rotation_age = 0`, then
`SELECT pg_reload_conf();`.

---

## 4. Implementation notes common to both drivers (`nodeNestloop.c`)

ROSL is implemented inside PostgreSQL's nested-loop executor node. The stock
node and ROSL share the same `NestLoopState`; ROSL's working state
(`RoslJoinState`) is allocated lazily the first time it runs.

### Behaviour is gated by a GUC — and a plan-shape guard

```
SET enable_rosl = off;   -- exact stock nested loop (default)
SET enable_rosl = on;    -- ROSL sampling join + running estimate, if the plan allows it
```

With `enable_rosl = off`, the node behaves exactly like the unmodified nested
loop. Even with it on, `ExecNestLoop` falls back to the exact stock path unless
the join is a plain **inner** join with a **non-parameterized** inner
(`nestParams == NIL`): ROSL rescans the whole inner and emits inner-join matches
only, so a parameterized nestloop or an outer join would silently produce wrong
results — the guard takes priority over the GUC.

### Correctness notes

- **Per-state RNG.** Cache draws use an `xorshift64*` stream seeded per
  `RoslJoinState` from high-resolution time × backend PID × state pointer — not
  process-global `rand()`/`srand()`. A shared, second-resolution seed would hand
  identically-timed concurrent workers identical "random" caches, correlating
  runs meant to be independent replicates and invalidating exactly the cross-run
  variance the intervals report.
- **Plan-shape guard.** Falls back to the stock join rather than sampling under
  an assumption that doesn't hold.
- **Exact outer population** (tiling mode). The summary uses `act_outer` — the
  exact sum of every M-block's size — for `|R|`; `|S|` stays the planner's
  `num_inner`, since this mode rescans `S` per M-block. (In single-M `act_outer`
  is just `m` and is a diagnostic — see §5.4.)

### Tunable constants

| Constant             | Value     | Meaning                                                                 |
|----------------------|-----------|-------------------------------------------------------------------------|
| `ROSL_M_LIM`         | 1588      | Outer (R) tuples per M-block (single-M: base for `m = M_LIM · M_MULT`)  |
| `ROSL_K_LIM`         | 1588      | Inner (body) tuples per K-block                                         |
| `ROSL_EXP_CACHE_LIM` | 529       | With-replacement draws per round (`L`)                                  |
| `ROSL_N_PROBES`      | 1588      | Exploration prefix `P` size (tiling mode only; unused in single-M)      |
| `ROSL_EPSILON`       | 0.2       | Fixed exploration mass (`ε`; no decay)                                  |
| `ROSL_EPSILON_FLOOR` | 0.2       | Propensity-decay floor (precondition for valid inference)               |
| `ROSL_FLAT_WEIGHTS`  | 1         | Tiling: 1 = flat `h = 1/pairs`; 0 = legacy stick-breaking A/B path      |
| `ROSL_ALPHA`         | 0.7       | Two-point allocation decay exponent (legacy tiling path / 2PT formula)  |
| `ROSL_TRAJ_CAP`      | 1,000,000 | Max per-round trajectory rows retained for the dump                     |
| **`ROSL_SINGLE_M`**  | **0**     | **1 = single-M estimator; 0 = classic tiling (default)**                |
| `ROSL_M_MULT`        | 1         | Single-M: scales `m = M_LIM · M_MULT` (test m ∈ {1×, 4×, 16×})          |
| `ROSL_STEERING`      | 0         | Single-M: 0 = uniform draw; 1 = joinability-weighted draw               |
| `ROSL_WEIGHT_MODE`   | FLAT      | Single-M: `FLAT` / `HADAD_CONST` / `HADAD_2PT` pooling weights          |
| `ROSL_VPRED_NUGGET`  | 1.0       | Single-M: fixed per-arm prior residual variance (formula constant)      |
| `ROSL_EMIT_MKEYS`    | 0         | Single-M diagnostic: dump M's join keys for the truth_M decomposition   |

`ROSL_ALPHA` is not a propensity knob under single-M — with the ε-floor
`e_t(r) ≥ ε/m` (`α = 0`), the exponent survives only as a fixed formula
constant inside the two-point allocation rate. `ROSL_VPRED_NUGGET` and the
numerical floors are formula constants, not tuning knobs: any positive value
preserves the I3 legality argument; magnitude affects only how much weight the
earliest (predictor-free) rounds receive.

---

## 5. The single-M driver (`ROSL_SINGLE_M = 1`)

### 5.1 Round protocol and legality invariants

`R` has `N_R` tuples, `S` has `N_S`. `M ⊂ R`, `|M| = m = M_LIM · M_MULT`, drawn
uniformly **once** in phase `PH_LOAD_M` (the first `m` outer tuples, uniform
under the physical shuffle, zero extra I/O). `S` is then streamed once in
K-blocks `B_1 … B_T`, `T = ⌈N_S/K⌉`. Round `t` (`finalize_round_single_m`):

1. Freeze the ε-smoothed sampling distribution `q_t` from rounds `1 … t−1` only
   (uniform when `ROSL_STEERING = 0`; joinability-weighted, ε-floored, when
   `1`). Freeze `q̂_t`.
2. Draw the split cache from `q_t`; inclusion probabilities
   `π_t(r) = 1 − (1 − q_t(r))^L`, all frozen for the round.
3. Probe the deduplicated cache against every `s ∈ B_t`; count per-arm matches
   `c_t(r)`.
4. Score `Ŷ_t` with the full-cache AIPW form (§1) and `v_t = ¼(corrA − corrB)²`.
5. Pool via `ROSL_WEIGHT_MODE` (§5.2); update per-arm accumulators (§5.3).
   Joinability/steering and running-noise updates land **strictly after** the
   round closes.

Enforced legality invariants:

- **I1** — `M` is drawn before any probing and never replaced.
- **I2** — `q_t`, `π_t` are frozen across the whole round; all weight/steering
  updates land between rounds. `reward[]`/`attempts[]` are updated during
  probing but feed `q_{t+1}`, never the frozen `q_t`.
- **I3** — `h_t` is `H_{t-1}`-measurable: `V_pred_t` is computed from `q_t` and
  completed-round statistics only, never from round `t`'s own outcome. This is
  the one correction vs the retired −34% build.
- **I4** — the horizon `T` entering `λ_t` is fixed at round 1 (from the inner
  planner estimate); `λ = 1` is forced on the true final round so the stick is
  consumed even if `T` was misestimated.
- **I5** — the Hadad self-normalized CI is fixed-horizon: `ci_within` /
  `ci_total` are valid (and coverage-scored) only on runs that exhaust the
  stream. The EB interval remains primary at a data-dependent stop.

### 5.2 Pooling weights — three modes (`ROSL_WEIGHT_MODE`)

`accumulate_round_single_m` emits exactly one branch (the mode is a compile
constant):

- **`FLAT`** — `h_t = 1/round_pairs`, the plain per-pair average and the shipped
  fallback. `V_pred`, the two-point mix, and `force_last` are cast away.
- **`HADAD_CONST`** — constant allocation `λ_t = 1/(T−t+1)` (paper Eq. 15).
- **`HADAD_2PT`** — the two-point allocation rate (paper Eq. 18), whose
  "stays-high vs decays" mixing weight `pi_mix` is the **mean frozen inclusion
  probability over all of M** (a pre-round, `H_{t-1}`-measurable quantity),
  **not** the legacy path's minimum over the realized cache (which depended on
  round `t`'s own draw and violated I3).

Both Hadad modes drive the stick-breaking recursion

```
h_t² · V_pred_t = stick · λ_t         (Eq. 12)
stick          -= h_t² · V_pred_t  = stick · λ_t   (exactly)
```

so with `λ = 1` forced on the true final round the stick telescopes to zero and
`Σ_t h_t² V_pred_t = 1` at `T`, independent of the `V_pred` values.

**Predicted variance (I3).** `V_pred_t = Σ_{r∈M} (m̂(r)² + nug)·(1−π_t(r))/π_t(r)`
with `m̂(r) = q̂(r)·|K|`, `π_t(r)` the **frozen** inclusion probability, and a
per-arm nugget `nug = ROSL_VPRED_NUGGET + vbar/m` where `vbar` is the running
mean of past within-round noise. The nugget is load-bearing: at round 1 no arm
has been attempted (`q̂ = 0` everywhere, `vbar = 0`), so without it `V_pred`
collapses to the numerical floor, `h_1` dwarfs every later weight, and the run
degenerates to a single-round estimate with a vacuous interval (observed live:
`lindeberg_max ~ 1e13`, `ci_within = 0`). A fixed constant is trivially
`H_{t-1}`-measurable and keeps `V_pred`'s units commensurate across rounds, so
legality is untouched — only weight efficiency depends on its value.

### 5.3 Per-arm accumulators and the two-stage interval

`M` is fixed, so `reward[]`/`attempts[]` accumulate across **every** round and
feed the frozen predictor. Two O(m) per-arm accumulators support the M-lottery
interval, updated in `finalize_round_single_m` over the cache:

```
A1(r) += 1[r ∈ C_t] · c_t(r)/π_t(r)          →  Q̂(r) = A1(r)/N_S
A2(r) += (1[r ∈ C_t] · c_t(r)/π_t(r))²       →  within-arm noise ν̂(r)
```

Memory is O(m) doubles, bounded by the same `work_mem` budget that sizes `M`.
`PrintRoslCounters` assembles the two-stage interval (`pop = num_outer · num_inner`):

- **Stage 1 (within-M):** `ci_within = ci_halfwidth`, the existing
  König–Huygens self-normalized (Hadad Eq. 11) half-width re-based on the
  weighted rounds. `p_m_hat = μ̂`.
- **Stage 2 (M-lottery):**
  `S²_between = max(0, sample-var_r(Q̂(r)) − mean_r ν̂(r))` (de-noised
  between-arm degree variance), `Var_stage2 = (1 − m/N_R)·S²_between/m`,
  `ci_between = 1.96·pop·√Var_stage2`.
- **Total:** `ci_total = √(ci_within² + ci_between²)` — the headline interval
  whose coverage against realized `J` is the number to score. `ci_within` alone
  is **expected** to under-cover on skewed cells by exactly the stage-2 term;
  that under-coverage is a prediction to verify, not a bug.

`n_blocks` / `ci_clust` / `ci_split` are degenerate here (one fixed M-block) and
fall back to `ci_within`. In the single-M mode the accuracy charts prefer
`ci_eb > ci_total > ci_halfwidth`; `ci_total` is fixed-horizon, so its coverage
is scored only on `exhausted` runs (I5).

### 5.4 Emitted fields

Single-M appends these numeric tokens to the standard `ROSL_SUMM` line (so the
worker's key=value tokenizer picks them up with no code change), keeping the
classic tokens verbatim:

```
… weight_mode=<0 FLAT|1 CONST|2 2PT> m_size=<m> ci_within=<half> ci_between=<half> ci_total=<half> p_m_hat=<p̂_M> t_rounds=<T actual> t_planned=<T planner>
```

`act_outer` is `m` (a diagnostic — the extrapolation scales by the planner
`num_outer = N_R`, not `m`). `t_planned` vs `t_rounds` makes any horizon
misestimation visible: if the planner underestimated `N_S`, rounds past `T` get
`h = 0` (excluded from the weighted point estimate but still folded into the EB
moments and the stage-2 accumulators); an overestimate is benign because
`force_last` consumes the stick on the true final (short) block.

**`ROSL_EMIT_MKEYS` diagnostic (default 0).** When 1, teardown emits `M`'s
outer join keys in chunked `ROSL_MKEYS k1,k2,…` lines (64 keys per line, a
handful of log lines) so the harness can compute
`truth_M = (N_R/m)·Σ deg_S(key)` and split the error into within-M and
M-lottery components. It **assumes the outer join key is attribute 1 of the
outer tuple and an integer-like by-value type** (true for the TPC-H equijoins
this targets); the `truth_M` query must use the same column.

### 5.5 State machine (`ExecRoslSingleM`)

- **`PH_LOAD_M`** — draw `M` once (first `m` outer tuples); empty outer →
  `PH_DONE`. Zero `reward[]`/`attempts[]`/`A1`/`A2` (never reset per block),
  start the timing clock, `ExecReScan` the inner, → `PH_STREAM_SBLOCK`.
- **`PH_STREAM_SBLOCK`** — load the next inner K-block; empty → `PH_DONE`.
  Build and freeze `q_t` (uniform, or joinability-weighted under steering),
  draw and dedup the split-tagged cache, add `|K|` to `attempts[]` for each
  uniquely cached tuple, reset per-round match counts, → `PH_STREAM_PROBE`.
- **`PH_STREAM_PROBE`** — probe the cache against the K-block, emitting matches
  one per call (resume cursors `probe_ci`/`probe_kj`); on the last pair,
  `finalize_round_single_m` folds the AIPW score, the I3-legal predicted
  variance, the per-arm accumulators, and the trajectory row, → `PH_STREAM_SBLOCK`.
- **`PH_DONE`** — inner exhausted; the cursor returns NULL.

Rescan resets to `PH_LOAD_M` (redraw `M`, restream `S`) and re-zeros the
per-arm and running-noise state so a rescanning parent gets a clean stage-2
estimate; `t_planned` is a fixed horizon and is **not** reset.

### 5.6 Risks and open questions

- **M-lottery dominance under skew.** The irreducible `(p_M − p)` variance
  scales as `S²_deg·(1−m/N_R)/m`; on Zipf cells it may dominate. Mitigation:
  size `m` to the `work_mem` ceiling (probe cost scales with `L`, not `m`, so
  large `m` is nearly free at run time), and let the error-budget gate set the
  required `m`. If no feasible `m` clears the accuracy bar on `z=1` cells, the
  design is rejected by data — itself a paper finding.
- **Horizon `T` needs `N_S`.** Stick-breaking needs `T` at round 1; the exact
  inner cardinality may not be known until the stream ends. Mitigation: take `T`
  from the inner planner estimate, force `λ = 1` on the true final round, and
  record `t_planned` vs `t_rounds`. `HADAD_CONST` is less sensitive to `T` than
  `HADAD_2PT` and is the fallback if sensitivity is material.
- **Steering is the weights' job.** With steering OFF the arms are exchangeable
  and flat pooling is optimal (the Hadad weights are vacuous); steering ON is
  what gives them a non-vacuous job. Steering ships OFF by default and has zero
  I/O penalty here because `M` is memory-resident.

---

## 6. Running it

### Configure the plan

ROSL must run as a plain, fixed-inner nested loop — the shape the §4 guard
requires. The benchmark worker sets (among others):

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
  caps concurrency, times each job, merges per-job CSVs into `trajectory.csv` /
  `summary.csv`, and tags the run mode at queue time so sweeps are
  self-describing.
- **`worker.py`** — for one configuration: computes ground truth once (forced
  hash join, cached in `truth_counts.json`), runs ROSL, drains its rows to force
  teardown, then reads this run's `ROSL_TRAJ` / `ROSL_SUMM` lines from the
  server log (isolated by backend PID + a per-run sentinel). When `ROSL_MKEYS`
  lines are present it computes `truth_M = (N_R/m)·Σ deg_S(key)` with one
  grouped SQL against the same schema, and emits the decomposition columns
  `truth_m`, `err_within` (`final_est − truth_M`), `err_between`
  (`truth_M − truth`). The summary schema also gains `weight_mode`, `m_size`,
  `ci_within`, `ci_between`, `ci_total`, `ci_total_covers_truth`.
- **`accuracy_charts.py`** — plots estimator error versus fraction of true
  output sampled, prefers CI bands in the order `ci_eb > ci_total >
  ci_halfwidth` (`ci_total` is fixed-horizon; the `--ci_col` override and the
  exhausted-only caveat carry over), and adds a per-cell decomposition chart
  stacking `|err_within|` and `|err_between|` against pct-of-truth.

Because the schema is additive, older per-job CSVs won't merge with new runs —
the manager's header-mismatch guard fails loudly, so give each sweep a fresh
results directory.

Output limits are applied per query and must stay in sync across the manager,
worker, and chart script:

| Query | LIMIT       |
|-------|-------------|
| Q9    | 221,700     |
| Q10   | 13,000      |
| Q11   | 327,624,700 |
| Q12   | 1,000       |
| Q15   | 43,800      |

> **Coverage caveat.** In the tiling mode, runs that hit these LIMITs stop on a
> rule correlated with the estimand and bias the point estimate low, so
> coverage-validation sweeps for `ci_halfwidth` should run without output
> limits. In single-M, output is confined to `M × S`, so paper-cap runs mostly
> end `exhausted` — conveniently the regime where the fixed-horizon `ci_within`
> / `ci_total` intervals are valid (I5).

### Trajectory CSV columns

```
size, query, zval, shuffle, repeat, round, pairs_seen, sample_matches,
mean_per_pair, est_join, truth, ratio, rel_error, elapsed_sec, delta_sec,
pct_of_truth_output
```

`ratio = est_join / truth` and `rel_error = (est_join − truth)/truth` are the
accuracy columns; `pct_of_truth_output` is the natural x-axis for
accuracy-vs-progress charts.

---

## 7. Decision gate and rollback

Adoption of single-M uses the same instrument that retired the adaptive-weights
version: a per-cell bias/RMSE/coverage table, single-M+weights vs the shipped
sequential-tiling flat baseline, at matched wall-clock, plus the simulation
gates (weights earn their keep only with steering ON; `ci_total` covers realized
`J` at nominal while `ci_within` alone under-covers on skewed cells; the
M-lottery error share fixes the required `m`). Adopt only if `ci_total` achieves
nominal coverage where the baseline's fixed-horizon CI cannot, and total RMSE
against realized `J` is no worse on the large majority of cells with no
catastrophic cell.

Everything lands behind `ROSL_SINGLE_M` / `ROSL_WEIGHT_MODE` with FLAT tiling as
the untouched default, all schema changes are additive, and the legacy A/B paths
stay compiled out but preserved — **rollback is a define flip**.

---

## 8. File map

| File                 | Role                                                                                                        |
|----------------------|-------------------------------------------------------------------------------------------------------------|
| `nodeNestloop.c`     | ROSL executor node: classic tiling driver + single-M driver, AIPW estimator, weight modes, interval family, log dump |
| `tpch_manager.py`    | Sweep manager: fan-out, timing, CSV merge, mode tag                                                          |
| `worker.py`          | Per-config runner: truth, ROSL run, server-log read, truth_M decomposition                                  |
| `accuracy_charts.py` | Accuracy + decomposition plots from merged `trajectory.csv`                                                  |