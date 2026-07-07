/*-------------------------------------------------------------------------
 *
 * nodeNestloop.c
 *	  Stock PostgreSQL nested loop, plus an opt-in ROSL block-nested-loop
 *	  sampling join with a running Horvitz-Thompson cardinality estimator.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeNestloop.c
 *
 *-------------------------------------------------------------------------
 *
 * BEHAVIOUR IS SELECTED BY THE GUC `enable_rosl` (default OFF):
 *
 * enable_rosl = off  -> exact stock nested loop.  This is what initdb,
 * the system catalogs, and every ordinary query
 * use, so normal database operation is unchanged.
 *
 * enable_rosl = on   -> ROSL sampling join + running estimate.  Turn this
 * on only for the experimental join queries you want
 * estimated:  SET enable_rosl = on;
 *
 * This is the ADAPTIVELY-WEIGHTED, TWO-PHASE ("probabilistic N-failure")
 * version (Section 7 of the ROSL alternatives note; estimator after Hadad,
 * Hirshberg, Zhan, Wager & Athey, "Confidence Intervals for Policy Evaluation
 * in Adaptive Experiments").  It supersedes the single-phase, uniform-pooled
 * raw-HT version.  Two things changed relative to that predecessor:
 *
 *   (A) SAMPLING is now two-phase per M-block.  The first n_probes tuples of
 *       the (randomly ordered) inner relation form a shared EXPLORATION PREFIX
 *       P.  Each M-block first runs one probabilistic exploration round: a
 *       UNIFORM cache over M is probed against all of P (region R x P).  The
 *       remaining inner tuples form the BODY B = S \ P, scanned in K-blocks
 *       exactly as before but with a FIXED epsilon (no decay: a dedicated
 *       exploration phase already supplies the initial joinability signal).
 *
 *   (B) The ESTIMATOR is now the adaptively-weighted AIPW estimator:
 *         - Per exploitation round the raw HT score is replaced by an AIPW
 *           score that uses the frozen average joinability q(t) as a control
 *           variate; under sparse joins this absorbs almost all the variance.
 *         - Per-round scores are pooled with variance-stabilizing evaluation
 *           weights h_r (stick-breaking recursion, two-point allocation rate)
 *           instead of uniform summation, restoring asymptotic normality.
 *         - A self-normalized variance is maintained in O(1) per round via
 *           three moment accumulators (Konig-Huygens form), so every round
 *           emits an approximate 95% confidence interval on J_hat.
 *
 * The epsilon-FLOOR is retained: it guarantees the propensity-decay lower
 * bound the CLT requires, so it is now a precondition for valid inference
 * rather than only a spike-damping heuristic.
 *
 * ============================ CORRECTIONS ============================
 * Three fixes ported from the corrected Fixed-Probe baseline; all apply here.
 *
 *  (1) RNG.  Uses a per-state xorshift64* PRNG seeded from a mix of high-
 *      resolution time, the backend PID, and the state pointer -- NOT process-
 *      global rand()/srand(time()).  The old 1-second srand() seed handed
 *      same-second workers (the concurrent harness launches many) IDENTICAL
 *      "random" caches, correlating runs meant to be independent replicates.
 *      That matters even more here than for Fixed-Probe: correlated replicates
 *      would invalidate the confidence-interval coverage this version reports.
 *
 *  (2) PLAN-SHAPE GUARD.  ExecNestLoop falls back to the exact stock path
 *      unless the join is a plain INNER join with a non-parameterised inner
 *      (nestParams == NIL).  The ROSL path rescans the inner per M-block and
 *      emits inner-join matches only, so without the guard a parameterised
 *      nestloop or outer join chosen by the planner would silently produce
 *      wrong results while the GUC is on.
 *
 *  (3) EXACT OUTER POPULATION.  The final extrapolation uses the ACTUAL outer
 *      size |R| = act_outer (summed over every M-block, hence exact once the
 *      scan finishes) instead of the planner's plan_rows.  The sampler only
 *      estimates a per-PAIR rate; the outer size is known exactly by teardown.
 *      NOTE ON |S|: unlike Fixed-Probe, this version does NOT materialise S --
 *      it rescans S and re-draws a fresh exploration prefix per M-block (the
 *      prefix is probabilistic and reweighted under ISPW, by design), so it
 *      never counts |S| exactly and still uses the planner's num_inner for the
 *      inner multiplier.  Correspondingly, the Fixed-Probe "negative-|B| cliff"
 *      and body-only multiplier do NOT arise here: both regions are HT-
 *      estimated and the multiplier is the full inner size, per the two-region
 *      probabilistic estimate (progress doc section 6.c).
 * ====================================================================
 *
 * Matching pairs are emitted immediately (one per call via resume cursors
 * probe_ci / probe_kj, and the exploration-phase analogues explore_ci /
 * explore_pj).  Only sampled rows are emitted; this is an approximate join
 * intended for cardinality estimation workloads.
 *
 * INTEGRATION (unchanged):
 * - execnodes.h: add `void *rosl;` to struct NestLoopState.
 * - guc.c: register the bool GUC `enable_rosl` (see notes at end of file).
 * - nodeNestloop.h: unchanged.
 *
 * ROSL ALGORITHM NOTES:
 * R (outer) is consumed in M-blocks of ROSL_M_LIM tuples.  For each M-block:
 *   1. EXPLORATION: load the first n_probes inner tuples as prefix P; draw a
 *      uniform exploit cache over M and probe it against all of P.  This
 *      round's inclusion probability is pi_exp = 1 - (1 - 1/Mn)^L, identical
 *      for every tuple, and its raw-HT score covers region R x P.
 *   2. EXPLOITATION: scan the body B = S \ P in K-blocks.  Each (M-block,
 *      K-block) round draws a joinability-weighted, epsilon-smoothed exploit
 *      cache and probes it against the K-block, emitting matching rows
 *      directly, and folds an AIPW score covering its slice of R x B.
 * Cache fill is one cumulative-probability pass plus a binary search per draw;
 * deduplication is a sort of the drawn indices and a single linear pass.
 * Assumes a non-parameterised inner (nestParams == NIL) and targets
 * INNER-join cardinality.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>
#include <time.h>

#include "executor/execdebug.h"
#include "executor/executor.h"
#include "executor/nodeNestloop.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

/*
 * GUC.  Defined here, registered in src/backend/utils/misc/guc.c.  Default
 * false keeps this node behaving exactly like the stock nested loop.
 */
bool		enable_rosl = false;

/* ---- ROSL tunables (block sizes and cache budget) ---- */
#define ROSL_M_LIM			1588	/* outer (R) tuples per M-block          */
#define ROSL_K_LIM			1588	/* inner (S) tuples per K-block          */
#define ROSL_EXP_CACHE_LIM	529		/* with-replacement draws per round (L)  */
#define ROSL_EPSILON_FLOOR  0.2		/* Minimum exploration threshold         */

/*
 * Two-phase / adaptively-weighted tunables.
 *
 * ROSL_N_PROBES: size of the shared exploration prefix P = S[0 : n_probes].
 * These inner tuples are consumed once per M-block for the probabilistic
 * exploration round and are EXCLUDED from every exploitation K-block (global
 * without-replacement), so no (t,s) pair is probed in both phases.
 *
 * ROSL_EPSILON: fixed exploration mass for the exploitation phase.  Unlike the
 * single-phase predecessor there is no decay: the exploration prefix already
 * supplies the initial joinability signal, so epsilon is held constant (and
 * must stay >= the implied floor for the CLT's propensity-decay bound).
 *
 * ROSL_ALPHA: propensity-decay exponent used by the two-point allocation rate
 * (paper Eq. 18 / Theorem 3).  Must lie in [0, 1); smaller values downweight
 * late rounds less aggressively.
 */
#define ROSL_N_PROBES		1588	/* inner tuples in exploration prefix P  */
#define ROSL_EPSILON		0.2		/* fixed exploitation exploration mass   */
#define ROSL_ALPHA			0.7		/* two-point allocation decay exponent
									 * (LEGACY: only used when
									 * ROSL_FLAT_WEIGHTS is 0)               */
#define ROSL_FLAT_WEIGHTS	1		/* 1: h = 1/round_pairs.  Section-9
									 * post-mortem result: variance-tracking
									 * weights bias the pooled mean toward
									 * low-variance slices under block
									 * heterogeneity (inverse-variance
									 * weighting targets a precision-weighted
									 * mean, which equals the population mean
									 * only under a common-mean regime).  The
									 * proxy weights survived that mechanism
									 * only by being nearly flat; flat weights
									 * close the exposure and measured equal-
									 * or-better on every tested cell.
									 * 0: legacy stick-breaking/two-point
									 * path, kept for A/B reproduction.      */

/*
 * Per-round trajectory is buffered in C and dumped once at executor teardown
 * (see PrintRoslCounters), Saketh-style -- nothing is emitted mid-stream, so
 * the client never has to parse a NOTICE to recover accuracy or timing.  This
 * caps how many rounds we retain; if a run exceeds it we keep the first
 * ROSL_TRAJ_CAP rounds and flag truncation in the final dump.  At ~64 bytes a
 * row this is a few MB, comfortably inside the backend's memory budget.
 */
#define ROSL_TRAJ_CAP		1000000	/* max per-round trajectory rows retained */

/* State-machine phases for the streaming ROSL driver. */
typedef enum RoslPhase
{
	PH_NEW_MBLOCK,					/* load next outer block, restart S          */
	PH_EXPLORE,						/* load prefix P, draw uniform cache, probe  */
	PH_NEW_SBLOCK,					/* load next inner (body) block, draw cache  */
	PH_PROBE,						/* probe cache x K-block, emit matches       */
	PH_DONE							/* outer exhausted: sampling join complete   */
} RoslPhase;

/*
 * All ROSL + estimator working state.  Reached through NestLoopState->rosl,
 * allocated lazily the first time enable_rosl is on for this node.
 */
typedef struct RoslJoinState
{
	/* configuration */
	int			m_lim;
	int			k_lim;
	int			exp_cache_lim;
	int			n_probes;			/* size of exploration prefix P           */
	double		epsilon_fixed;		/* fixed exploitation epsilon (no decay)  */
	double		alpha;				/* two-point allocation decay exponent    */

	/* outer (R) block buffer */
	TupleTableSlot **m_slots;		/* [m_lim] copies of current M-block      */
	int			m_count;			/* tuples in current M-block (Mn)         */
	int		   *reward;				/* [m_lim] successful joins r(t) in block */
	int		   *attempts;			/* [m_lim] join attempts a(t) in block    */

	/* inner (S) body block buffer */
	TupleTableSlot **k_slots;		/* [k_lim] copies of current K-block      */
	int			k_count;			/* tuples in current K-block (Kn)         */

	/* shared exploration prefix P = S[0 : n_probes] (reloaded per M-block) */
	TupleTableSlot **p_slots;		/* [n_probes] copies of prefix tuples     */
	int			p_count;			/* tuples actually loaded into P          */

	/* per-round selection distribution and exploit cache */
	double	   *p;					/* [m_lim] single-draw prob (normalised)  */
	double	   *q;					/* [m_lim] frozen OPTIMISTIC joinability   */
	double	   *qhat;				/* [m_lim] frozen AIPW predictor (0 if new)*/
	double	   *cum;				/* [m_lim] cumulative of p                */
	int		   *interim;			/* [exp_cache_lim] drawn M-indices (dups) */
	int		   *cache_midx;			/* [exp_cache_lim] unique M-indices       */
	int			cache_count;		/* number of unique cached tuples         */
	bool	   *in_cache;			/* [m_lim] membership flag for the cache  */
	bool	   *in_half_a;			/* [m_lim] split-cache replicate A (draws
									 * 0..L/2-1 before dedup)                */
	bool	   *in_half_b;			/* [m_lim] split-cache replicate B (draws
									 * L/2..L-1 before dedup)                */

	/*
	 * ---- within-round variance track (Section-9-inspired, telemetry) ----
	 * The L with-replacement draws are i.i.d., so their two halves give two
	 * INDEPENDENT AIPW replicates Y_A, Y_B per round (each with its own
	 * inclusion probability at exponent L/2).  v = ((Y_A - Y_B)/2)^2 is an
	 * unbiased estimate of Var((Y_A+Y_B)/2 | H_{r-1}) -- a pure-noise,
	 * heterogeneity-free variance the full-cache design cannot otherwise
	 * observe.  The POINT ESTIMATOR is untouched (still the full-cache
	 * score); v feeds only var_noise (a noise-only variance track that lets
	 * the clustered SS be decomposed into noise vs cross-block truth
	 * heterogeneity) and the Lindeberg-style max telemetry.  v is a mild
	 * upper gauge for the full-cache score's own noise (pi at L/2 < pi at L).
	 */
	double		var_noise;			/* sum_r h_r^2 v_r                       */
	double		max_h2v;			/* max_r h_r^2 v_r (Lindeberg telemetry) */
	int		   *round_match;		/* [m_lim] matches this round, by M-index */

	/*
	 * epsilon is FIXED in this version (epsilon_fixed).  The old decaying
	 * epsilon field is gone; a dedicated exploration phase supplies the
	 * initial signal, and a constant epsilon keeps the propensity-decay bound
	 * that the adaptively-weighted CLT requires.
	 */

	/* ---- adaptively-weighted (AW) estimator accumulators ---- */
	double		est_num;			/* sum_r h_r * Y_hat_round (weighted)     */
	double		weight_sum;			/* sum_r h_r * (Mn*Kn) (weighted pairs)   */
	double		est_den;			/* sum_r (Mn*Kn): UNWEIGHTED pairs seen   */
	double		mom_yy;				/* M1 = sum_r h_r^2 * Yhat^2              */
	double		mom_yp;				/* M2 = sum_r h_r^2 * Yhat * pairs        */
	double		mom_pp;				/* M3 = sum_r h_r^2 * pairs^2             */
	double		stick;				/* stick-breaking residual 1 - sum h^2 V  */

	/*
	 * ---- Empirical-Bernstein confidence sequence (Section 8 guard band) ----
	 * Anytime-valid interval on the bounded per-round rate X_r = Yhat/pairs.
	 * Needs no variance convergence and remains meaningful at a data-dependent
	 * stop (output LIMIT) -- the one residue the fixed-horizon CI cannot cover.
	 * Three O(1) accumulators; emitted alongside (never instead of) the
	 * self-normalized CI.
	 */
	double		eb_sx;				/* sum_r X_r                              */
	double		eb_sxx;				/* sum_r X_r^2                            */
	double		eb_max;				/* max_r |X_r| (Bernstein range bound B)  */
	long		eb_n;				/* rounds folded into the EB moments      */

	/*
	 * ---- M-block-clustered / split-half variance (Section 7.h) ----
	 * Rounds within one M-block share adaptive feedback through the block's
	 * accumulated q, so the M-block -- not the round -- is the honest
	 * independence unit.  We keep two block-level variance estimates and emit
	 * both as a bracket:
	 *   clustered  : per-block residual (B_Y - mu B_W); repairs the
	 *                within-block feedback correlation but conflates
	 *                cross-block truth heterogeneity with noise (over-wide
	 *                under skew).
	 *   split-half : per-block d_b = Y_A - (W_A/W_B) Y_B over odd/even rounds;
	 *                cancels the block's truth level (heterogeneity-free) but
	 *                shrinks under between-half feedback (anti-conservative in
	 *                the clustered CI's target regime).
	 * Both are O(1) streaming: we hold the CURRENT block's partial sums and
	 * fold a squared residual into a running SS at each block boundary.
	 */
	double		blk_ynum;			/* current block: sum h*Yhat              */
	double		blk_wpair;			/* current block: sum h*pairs             */
	double		blk_ya;				/* current block, ODD rounds: sum h*Yhat  */
	double		blk_wa;				/* current block, ODD rounds: sum h*pairs */
	double		blk_yb;				/* current block, EVEN rounds: sum h*Yhat */
	double		blk_wb;				/* current block, EVEN rounds: sum h*pairs*/
	int			blk_round;			/* round index within the current block   */
	bool		blk_open;			/* whether a block is currently open      */
	double		clust_ss;			/* running sum of clustered residual^2    */
	double		split_ss;			/* running sum of split-half d_b^2        */
	long		n_blocks;			/* completed M-blocks folded into SS      */
	long		round_idx;			/* flat round index r across all blocks   */
	double		t_est;				/* estimated total number of rounds       */
	double		num_outer;			/* |R| (planner row estimate)             */
	double		num_inner;			/* |S| (planner row estimate)             */
	double		act_outer;			/* |R| ACTUAL: running sum of m_count      */

	/* PH_PROBE resume cursors (persist across calls within one round) */
	int			probe_ci;			/* cache index to resume from                */
	int			probe_kj;			/* K-block (body) index to resume from       */

	/* PH_EXPLORE resume cursors (persist across calls within one round) */
	int			explore_ci;			/* exploration cache index to resume from    */
	int			explore_pj;			/* prefix index to resume from               */
	bool		explore_built;		/* whether this M-block's explore round is set*/

	/* state machine + diagnostics */
	RoslPhase	phase;
	long		rounds;
	long		t_steps;			/* total predicate evaluations            */
	long		sample_matches;		/* cache matches observed so far          */

	/*
	 * Per-round trajectory, buffered in C and dumped once at teardown.
	 * Replaces the old per-round elog(NOTICE) that the Python client used to
	 * parse off the wire mid-stream.  Each array is [ROSL_TRAJ_CAP]; index
	 * traj_count is the next free slot.  Parallel arrays (rather than an array
	 * of structs) keep the dump loop trivial and avoid a second typedef.
	 */
	long	   *traj_round;			/* round number (1-based)                 */
	double	   *traj_mean_per_pair;	/* mu_hat at this round                   */
	double	   *traj_est_join;		/* J_hat at this round                    */
	double	   *traj_ci_halfwidth;	/* 95% CI half-width on J_hat             */
	double	   *traj_ci_eb;			/* EB-CS guard-band half-width (Sec. 8);
									 * the anytime interval a mid-run look or
									 * LIMIT-stopped run should read           */
	double	   *traj_pairs_seen;	/* est_den (cumulative Mn*Kn)             */
	long	   *traj_sample_matches;/* cumulative sample_matches at this round */
	double	   *traj_elapsed_ms;	/* ms from first-row start to this round  */
	int			traj_count;			/* rounds recorded so far                 */
	bool		traj_truncated;		/* set if we hit ROSL_TRAJ_CAP            */

	/* timing anchors (in-C, source-measured -- not libpq flush time) */
	TimestampTz	t_start;			/* set when the first M-block is loaded   */
	bool		t_started;			/* whether t_start has been set yet       */

	/* per-state PRNG (xorshift64*) -- independent stream per backend/node    */
	uint64		rng_state;			/* never 0; seeded in rosl_state_init      */
} RoslJoinState;


/* ----------------------------------------------------------------
 *		ROSL helpers
 * ----------------------------------------------------------------
 */

/* qsort comparator for plain ints (the drawn M-indices). */
static int
cmp_int(const void *a, const void *b)
{
	int			x = *(const int *) a;
	int			y = *(const int *) b;

	return (x > y) - (x < y);
}

/*
 * Per-state PRNG: xorshift64* -> uniform double in [0,1).  Replaces process-
 * global rand()/srand(), which is low quality and shares one seed across the
 * whole process -- so concurrently launched workers seeded from a 1-second
 * clock drew identical caches, correlating runs meant to be independent
 * replicates and biasing exactly the cross-run variance the experiment (and,
 * here, the confidence-interval coverage) is meant to measure.  Seeded once
 * per state in rosl_state_init from time x PID x pointer; the state is kept
 * non-zero there (xorshift cannot recover from an all-zero state).
 */
static inline double
rosl_rand_double(RoslJoinState *st)
{
	uint64		x = st->rng_state;

	x ^= x >> 12;
	x ^= x << 25;
	x ^= x >> 27;
	st->rng_state = x;
	/* xorshift64* scramble, then take the top 53 bits -> [0,1) */
	return (double) ((x * UINT64CONST(0x2545F4914F6CDD1D)) >> 11)
		* (1.0 / 9007199254740992.0);
}

/*
 * lower_bound over a non-decreasing cumulative array: return the smallest
 * index i with cum[i] >= key.  cum[n-1] is pinned to 1.0 by the caller, so
 * for any key in [0,1) the result is always a valid index in [0, n-1].
 */
static int
lower_bound_cum(const double *cum, int n, double key)
{
	int			lo = 0;
	int			hi = n - 1;
	int			ans = n - 1;

	while (lo <= hi)
	{
		int			mid = lo + (hi - lo) / 2;

		if (cum[mid] >= key)
		{
			ans = mid;
			hi = mid - 1;
		}
		else
			lo = mid + 1;
	}
	return ans;
}

/* Pull up to m_lim outer tuples into the M-block buffer; return the count. */
static int
load_outer_block(RoslJoinState *st, PlanState *outerPlan)
{
	int			n = 0;

	while (n < st->m_lim)
	{
		TupleTableSlot *slot = ExecProcNode(outerPlan);

		if (TupIsNull(slot))
			break;
		ExecCopySlot(st->m_slots[n], slot);
		n++;
	}
	return n;
}

/* Pull up to k_lim inner tuples into the K-block buffer; return the count. */
static int
load_inner_block(RoslJoinState *st, PlanState *innerPlan)
{
	int			n = 0;

	while (n < st->k_lim)
	{
		TupleTableSlot *slot = ExecProcNode(innerPlan);

		if (TupIsNull(slot))
			break;
		ExecCopySlot(st->k_slots[n], slot);
		n++;
	}
	return n;
}

/*
 * Pull up to n_probes inner tuples into the exploration prefix buffer P.
 * Called once per M-block, immediately after ExecReScan(innerPlan), so P is
 * always the first n_probes tuples of the (randomly ordered) inner relation.
 * The subsequent body scan (load_inner_block) resumes from tuple n_probes+1,
 * so P and the body are disjoint (global without-replacement).
 */
static int
load_prefix_block(RoslJoinState *st, PlanState *innerPlan)
{
	int			n = 0;

	while (n < st->n_probes)
	{
		TupleTableSlot *slot = ExecProcNode(innerPlan);

		if (TupIsNull(slot))
			break;
		ExecCopySlot(st->p_slots[n], slot);
		n++;
	}
	return n;
}

/*
 * Custom epsilon-greedy smoothing over the current M-block, using the average
 * joinability accumulated so far in this M-block and the current epsilon.
 *
 * For each tuple t, the average joinability is
 *
 *   q(t) = r(t) / a(t)   if a(t) > 0   (successes over attempts so far)
 *        = 1            if a(t) = 0   (untried tuples treated as fully joinable)
 *
 * Let Qtotal = sum over t in A of q(t).  The single-draw distribution is
 *
 *   p(t) = (1-eps) * q(t)/Qtotal + eps/|A|   if Qtotal > 0
 *        = 1/|A|                             if Qtotal = 0
 *
 * The Qtotal = 0 case (every tuple has been tried and none has joined yet)
 * falls back to a uniform draw, since there is no joinability signal left to
 * weight by.  The result is normalised to a proper distribution.
 */
static void
build_distribution(RoslJoinState *st, bool uniform)
{
	int			n = st->m_count;
	int			t;
	double		eps = st->epsilon_fixed;
	double		Mn = (double) n;
	double		Qtot = 0.0;
	double		sum = 0.0;

	/*
	 * Always (re)compute and FREEZE q(t) into st->q[] first.  This is the
	 * average joinability used both to weight the exploitation draw and as the
	 * AIPW control variate m_hat(t) in finalize_round().  Freezing it here,
	 * before the round is probed, keeps it H_{t-1}-measurable (it depends only
	 * on attempts/successes from prior rounds), which is what the AIPW
	 * unbiasedness argument requires.
	 *
	 *   q(t) = r(t) / a(t)   if a(t) > 0
	 *        = 1            if a(t) = 0   (optimistic: untried => fully joinable)
	 */
	for (t = 0; t < n; t++)
	{
		double		qt;

		if (st->attempts[t] == 0)
		{
			qt = 1.0;				/* optimistic value used for SELECTION    */
			st->qhat[t] = 0.0;		/* but predict ZERO matches for AIPW      */
		}
		else
		{
			qt = (double) st->reward[t] / (double) st->attempts[t];
			st->qhat[t] = qt;		/* frozen match-rate predictor            */
		}

		st->q[t] = qt;
		Qtot += qt;
	}

	/*
	 * Exploration round: draw UNIFORMLY over M (every tuple gets the same
	 * inclusion probability pi_exp).  No joinability weighting, no epsilon.
	 */
	if (uniform)
	{
		for (t = 0; t < n; t++)
			st->p[t] = 1.0 / Mn;
		return;
	}

	/*
	 * Exploitation round: epsilon-greedy on joinability with FIXED epsilon.
	 *   p(t) = (1-eps) * q(t)/Qtot + eps/|A|   if Qtot > 0
	 *        = 1/|A|                            if Qtot = 0
	 */
	for (t = 0; t < n; t++)
	{
		double		pt;

		if (Qtot > 0.0)
			pt = (1.0 - eps) * (st->q[t] / Qtot) + eps / Mn;
		else
			pt = 1.0 / Mn;
		st->p[t] = pt;
		sum += pt;
	}

	if (sum > 0.0)
	{
		for (t = 0; t < n; t++)
			st->p[t] /= sum;
	}
	else
	{
		for (t = 0; t < n; t++)
			st->p[t] = 1.0 / Mn;
	}
}

/* One linear pass to build the cumulative distribution; pin the last to 1.0. */
static void
build_cumulative(RoslJoinState *st)
{
	int			n = st->m_count;
	int			t;
	double		run = 0.0;

	for (t = 0; t < n; t++)
	{
		run += st->p[t];
		st->cum[t] = run;
	}
	st->cum[n - 1] = 1.0;			/* guard against FP drift so draws land  */
}

/*
 * Draw exp_cache_lim M-indices WITH replacement (binary search over the
 * cumulative array), then SORT the drawn indices and keep one per run to
 * obtain the unique exploit cache.
 */
static void
draw_and_dedup_cache(RoslJoinState *st)
{
	int			L = st->exp_cache_lim;
	int			n = st->m_count;
	int			s;

	/* with-replacement draws, each an O(log M) binary search */
	for (s = 0; s < L; s++)
	{
		double		r = rosl_rand_double(st);	/* [0,1), per-state stream */

		st->interim[s] = lower_bound_cum(st->cum, n, r);
	}

	/*
	 * Split-cache replicates: record which HALF each draw belongs to before
	 * the sort destroys draw order.  Draws are i.i.d., so halves A and B are
	 * two independent with-replacement samples of size L/2 and L-L/2; each
	 * half's dedup has inclusion probability 1-(1-p)^(L/2) etc.  Used only by
	 * the within-round variance track in finalize_*_round().
	 */
	memset(st->in_half_a, 0, sizeof(bool) * n);
	memset(st->in_half_b, 0, sizeof(bool) * n);
	{
		int			La = L / 2;

		for (s = 0; s < L; s++)
		{
			if (s < La)
				st->in_half_a[st->interim[s]] = true;
			else
				st->in_half_b[st->interim[s]] = true;
		}
	}

	/* sort by M-index, then dedup in a single linear pass */
	qsort(st->interim, L, sizeof(int), cmp_int);

	/*
	 * Clear membership flags for the whole M-block, then set them for the
	 * unique cached indices.  in_cache[] lets finalize_round() add the AIPW
	 * baseline term q(t)*|K| for tuples NOT in the cache without a second sort
	 * or search (one linear pass over M instead).
	 */
	memset(st->in_cache, 0, sizeof(bool) * n);

	st->cache_count = 0;
	for (s = 0; s < L; s++)
	{
		if (s == 0 || st->interim[s] != st->interim[s - 1])
		{
			int			idx = st->interim[s];

			st->cache_midx[st->cache_count++] = idx;
			st->in_cache[idx] = true;
		}
	}
}

#if !ROSL_FLAT_WEIGHTS
/*
 * Two-point allocation rate for round r (paper Eq. 18 / Theorem 3), computed
 * in closed form (no scan over future rounds).  pi_repr in [0,1] mixes the
 * "stays high" case (lambda_const) and the "decays" case (lambda_decay); the
 * result is clamped into the Theorem-3 bounds and pinned to 1 on the last
 * round so the stick-breaking residual is fully consumed.
 */
static double
two_point_lambda(RoslJoinState *st, long r, double pi_repr)
{
	double		T = st->t_est;
	double		a = st->alpha;
	double		rr = (double) r;
	double		lambda_const;
	double		lambda_decay;
	double		lambda;
	double		lo,
				hi;
	double		denom_decay;

	if (T < rr)
		T = rr;						/* guard: never let T fall below r        */

	/* "stays high" branch: 1 / (T - r + 1) */
	lambda_const = 1.0 / (T - rr + 1.0);

	/* "decays" branch: r^-a / (r^-a + (T^{1-a} - r^{1-a})/(1-a)) */
	denom_decay = pow(rr, -a)
		+ (pow(T, 1.0 - a) - pow(rr, 1.0 - a)) / (1.0 - a);
	if (denom_decay > 0.0)
		lambda_decay = pow(rr, -a) / denom_decay;
	else
		lambda_decay = lambda_const;

	lambda = pi_repr * lambda_const + (1.0 - pi_repr) * lambda_decay;

	/* Theorem-3 bounds: lower = 1/(T-r+1); upper = C * lambda_decay-ish term. */
	lo = 1.0 / (T - rr + 1.0);
	hi = (denom_decay > 0.0) ? (pow(rr, -a) / denom_decay) : 1.0;
	if (hi < lo)
		hi = lo;
	if (lambda < lo)
		lambda = lo;
	if (lambda > hi)
		lambda = hi;

	/* last round: consume the whole remaining stick */
	if (rr >= T)
		lambda = 1.0;

	return lambda;
}
#endif							/* !ROSL_FLAT_WEIGHTS */

/*
 * ACCUMULATE (paper subroutine): fold one round's HT/AIPW score into the
 * adaptively-weighted running estimate in O(1).
 *
 *   V_r          = round_pairs / pi_repr        (conditional-variance proxy)
 *   h_r^2        = (stick * lambda_r) / V_r     (stick-breaking, Eq. 12)
 *   stick       -= h_r^2 * V_r                  (= stick * (1 - lambda_r))
 *   est_num     += h_r * Yhat
 *   weight_sum  += h_r * round_pairs
 *   mom_yy/yp/pp updated for the Konig-Huygens variance (Eq. 11), all O(1).
 *
 * est_den accumulates the UNWEIGHTED pair count so the summary/denominator
 * bookkeeping still reports pairs seen; the point estimate uses weight_sum.
 */
/*
 * Two-sided t critical value at level 0.05 for df degrees of freedom.  Sparse
 * lookup table with linear-in-1/df interpolation between anchors and a clamp
 * to the normal value (1.96) for large df.  Used only by the block-clustered
 * and split-half CIs, whose df is the (small) number of M-blocks minus one.
 */
static double
t_crit_975(long df)
{
	static const long	dfs[]  = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
								  12, 15, 20, 25, 30, 40, 60, 120};
	static const double	tvs[]  = {12.706, 4.303, 3.182, 2.776, 2.571,
								  2.447, 2.365, 2.306, 2.262, 2.228,
								  2.179, 2.131, 2.086, 2.060, 2.042,
								  2.021, 2.000, 1.980};
	const int	n = (int) (sizeof(dfs) / sizeof(dfs[0]));
	int			i;

	if (df <= 1)
		return tvs[0];
	if (df >= dfs[n - 1])
		return 1.96;					/* normal limit for large df */
	for (i = 1; i < n; i++)
	{
		if (df <= dfs[i])
		{
			/* interpolate in 1/df, which is where the t curve is ~linear */
			double		x0 = 1.0 / (double) dfs[i - 1];
			double		x1 = 1.0 / (double) dfs[i];
			double		x  = 1.0 / (double) df;
			double		w  = (x - x0) / (x1 - x0);

			return tvs[i - 1] + w * (tvs[i] - tvs[i - 1]);
		}
	}
	return 1.96;
}

/*
 * Close the currently-open M-block: fold its clustered residual and its
 * split-half difference into the running SS accumulators.  Called at every
 * M-block boundary (and once at teardown for the final block).  O(1).
 *
 *   clustered  : d = blk_ynum - mu * blk_wpair, with mu the CURRENT running
 *                weighted mean.  Because mu drifts slightly as later blocks
 *                arrive, this is an approximation to the fixed-mu residual;
 *                the error is O(1/n_blocks) and vanishes as blocks accumulate.
 *   split-half : d = blk_ya - (blk_wa / blk_wb) * blk_yb, matching the two
 *                halves' weighted pair masses so the block's density level
 *                cancels.  Requires both halves non-empty (>=2 rounds in the
 *                block); single-round blocks contribute to clustered only.
 */
static void
close_block(RoslJoinState *st)
{
	double		mu;
	double		dc;

	if (!st->blk_open)
		return;

	if (st->weight_sum > 0.0)
	{
		mu = st->est_num / st->weight_sum;
		dc = st->blk_ynum - mu * st->blk_wpair;
		st->clust_ss += dc * dc;

		if (st->blk_wa > 0.0 && st->blk_wb > 0.0)
		{
			double		ds = st->blk_ya - (st->blk_wa / st->blk_wb) * st->blk_yb;

			st->split_ss += ds * ds;
		}
		st->n_blocks++;
	}

	/* reset per-block partials for the next block */
	st->blk_ynum = st->blk_wpair = 0.0;
	st->blk_ya = st->blk_wa = 0.0;
	st->blk_yb = st->blk_wb = 0.0;
	st->blk_round = 0;
	st->blk_open = false;
}

static void
accumulate_round(RoslJoinState *st, double round_pairs, double Yhat,
				 double pi_repr, double v_round)
{
	double		h2;
	double		h;

	st->round_idx++;

#if ROSL_FLAT_WEIGHTS
	/*
	 * Flat weights: h = 1/round_pairs, so the pooled mu is the plain
	 * per-pair average of round scores.  Weights carry NO variance
	 * information by design -- see the ROSL_FLAT_WEIGHTS comment at the
	 * definition site for the mechanism (inverse-variance weighting under
	 * block heterogeneity biases the estimand toward low-variance slices).
	 * pi_repr is retained in the signature for the legacy path only.
	 */
	(void) pi_repr;
	if (round_pairs <= 0.0)
		return;						/* callers guarantee > 0; defensive      */
	h = 1.0 / round_pairs;
	h2 = h * h;
#else
	{
		double		V_r;
		double		lambda;

		if (pi_repr <= 0.0)
			pi_repr = 1e-12;		/* numerical guard; epsilon-floor bounds it */

		V_r = round_pairs / pi_repr;
		if (V_r <= 0.0)
			V_r = 1e-12;

		lambda = two_point_lambda(st, st->round_idx, pi_repr);

		h2 = (st->stick * lambda) / V_r;
		if (h2 < 0.0)
			h2 = 0.0;				/* stick underflow guard                   */
		h = sqrt(h2);

		st->stick -= h2 * V_r;
		if (st->stick < 0.0)
			st->stick = 0.0;
	}
#endif

	st->est_num += h * Yhat;
	st->weight_sum += h * round_pairs;
	st->est_den += round_pairs;

	/* moment accumulators for the recentered variance (all O(1)) */
	st->mom_yy += h2 * Yhat * Yhat;
	st->mom_yp += h2 * Yhat * round_pairs;
	st->mom_pp += h2 * round_pairs * round_pairs;

	/*
	 * Within-round (noise-only) variance track from the split-cache
	 * replicates, plus the Lindeberg-style max telemetry the design notes
	 * asked for (Fix 3): a production run can certify that no single round
	 * dominates the variance by inspecting max_h2v against var_noise.
	 */
	{
		double		hv = h2 * v_round;

		st->var_noise += hv;
		if (hv > st->max_h2v)
			st->max_h2v = hv;
	}

	/*
	 * Empirical-Bernstein moments on the bounded per-round rate X = Yhat/pairs
	 * (Section 8 guard band).  Both call sites guarantee round_pairs > 0
	 * (finalize_explore_round returns early on an empty prefix; PH_NEW_SBLOCK
	 * skips finalize_round when k_count == 0), but guard anyway so a future
	 * call site cannot divide by zero.  Note the EB estimand is the UNWEIGHTED
	 * mean of per-round rates: with unequal round sizes (the last K-block is
	 * short) this weights small slices slightly more than total/total-pairs
	 * does; the discrepancy is bounded by one round's share and vanishes as
	 * rounds accumulate.
	 */
	if (round_pairs > 0.0)
	{
		double		X = Yhat / round_pairs;
		double		ax = (X < 0.0) ? -X : X;

		st->eb_sx += X;
		st->eb_sxx += X * X;
		if (ax > st->eb_max)
			st->eb_max = ax;
		st->eb_n++;
	}

	/*
	 * Block-level partials for the clustered / split-half variance.  A block
	 * is opened lazily on its first round; the M-block driver calls
	 * close_block() at each boundary.  Odd/even rounds within the block feed
	 * the two split-half halves.
	 */
	st->blk_open = true;
	st->blk_ynum += h * Yhat;
	st->blk_wpair += h * round_pairs;
	if ((st->blk_round & 1) == 0)
	{
		st->blk_ya += h * Yhat;
		st->blk_wa += h * round_pairs;
	}
	else
	{
		st->blk_yb += h * Yhat;
		st->blk_wb += h * round_pairs;
	}
	st->blk_round++;
}

/*
 * Record the current running estimate (and its confidence interval) into the
 * in-C trajectory buffer.  Called after each accumulate_round().  Nothing
 * crosses the wire here; the whole trajectory is dumped once at teardown by
 * PrintRoslCounters() (Saketh communication model).
 *
 * mu_hat  = est_num / weight_sum                    (weighted mean per pair)
 * J_hat   = mu_hat * |R| * |S|
 * SS      = M1 - 2 mu_hat M2 + mu_hat^2 M3          (Konig-Huygens, O(1))
 * V_hat   = SS / weight_sum^2                        (self-normalized, Eq. 11)
 * CI      = J_hat +/- 1.96 * |R| * |S| * sqrt(V_hat)
 */
static void
record_trajectory(RoslJoinState *st)
{
	double		mu;
	double		Jhat;
	double		ss;
	double		Vhat;
	double		ci_half;
	double		ci_eb = 0.0;

	st->rounds++;

	if (st->weight_sum <= 0.0)
		return;

	mu = st->est_num / st->weight_sum;
	Jhat = mu * st->num_outer * st->num_inner;

	/* recentered weighted sum of squares; clamp tiny negatives from FP noise */
	ss = st->mom_yy - 2.0 * mu * st->mom_yp + mu * mu * st->mom_pp;
	if (ss < 0.0)
		ss = 0.0;
	Vhat = ss / (st->weight_sum * st->weight_sum);
	ci_half = 1.96 * st->num_outer * st->num_inner * sqrt(Vhat);

	/*
	 * Per-round EB-CS guard band (Section 8), O(1) from the running moments.
	 * This is the interval a mid-run look -- or a run about to be stopped by
	 * an output LIMIT -- should read: the self-normalized ci_half above is
	 * fixed-horizon-only.  Uses |R|*|S| (planner inner) like ci_half; the
	 * teardown ROSL_SUMM recomputes both against the exact act_outer.
	 */
	if (st->eb_n > 1)
	{
		double		xb = st->eb_sx / (double) st->eb_n;
		double		vx = st->eb_sxx / (double) st->eb_n - xb * xb;
		double		lg = 3.6888794541139363;	/* ln(2/0.05) */
		double		half_rate;

		if (vx < 0.0)
			vx = 0.0;
		half_rate = sqrt(2.0 * vx * lg / (double) st->eb_n)
			+ (7.0 / 3.0) * st->eb_max * lg / (double) st->eb_n;
		ci_eb = half_rate * st->num_outer * st->num_inner;
	}

	if (st->traj_count < ROSL_TRAJ_CAP)
	{
		double		elapsed_ms = 0.0;

		if (st->t_started)
		{
			/*
			 * TimestampTz is an int64 count of microseconds; subtracting two
			 * of them directly avoids depending on the platform's
			 * TimestampDifference() out-param signature (long vs int64), which
			 * has changed across PG versions.
			 */
			TimestampTz now = GetCurrentTimestamp();

			elapsed_ms = (double) (now - st->t_start) / 1000.0;
		}

		st->traj_round[st->traj_count]          = st->rounds;
		st->traj_mean_per_pair[st->traj_count]  = mu;
		st->traj_est_join[st->traj_count]       = Jhat;
		st->traj_ci_halfwidth[st->traj_count]   = ci_half;
		st->traj_ci_eb[st->traj_count]          = ci_eb;
		st->traj_pairs_seen[st->traj_count]     = st->est_den;
		st->traj_sample_matches[st->traj_count] = st->sample_matches;
		st->traj_elapsed_ms[st->traj_count]     = elapsed_ms;
		st->traj_count++;
	}
	else
		st->traj_truncated = true;
}

/*
 * Finalise a completed EXPLOITATION round (one M-block x one body K-block).
 *
 * The per-round score is the AIPW estimator for the total match count in the
 * M x K slice:
 *
 *   Y_hat = sum_{t in M} mhat(t)                         (baseline, all of M)
 *         + sum_{t in cache} (round_match(t) - mhat(t)) / pi(t)   (correction)
 *
 * with pi(t) = 1 - (1 - p(t))^L the exact inclusion probability.  The control
 * variate mhat(t) must be a genuine PREDICTOR of this round's match count, so
 * it is the empirical joinability RATE times the block size:
 *
 *   mhat(t) = qhat(t) * Kn
 *
 * where qhat(t) is FROZEN in build_distribution() before the round is probed
 * (so it is H_{t-1}-measurable, as AIPW unbiasedness requires): it is the
 * empirical match rate reward(t)/attempts(t), or 0 for a tuple with no prior
 * attempts.  This is deliberately NOT the selection value q(t): selection uses
 * the OPTIMISTIC q(t)=1 for untried tuples (to keep exploring them), but as a
 * regression predictor an untried tuple must predict ZERO matches -- using q=1
 * here would make the baseline claim every untried tuple joins all of K, which
 * the correction term would then subtract back with a huge inverse-pi factor,
 * injecting exactly the variance the control variate is meant to remove.  When
 * most pairs do not join, qhat(t) ~ 0, the baseline ~ 0, and the inverse-pi
 * correction fires only on the rare actual matches.
 *
 * The round is then pooled by accumulate_round().  Under ROSL_FLAT_WEIGHTS
 * (the default) the pooling weight is flat (h = 1/round_pairs) and pi_repr is
 * carried only for the legacy stick-breaking path; the split-cache half
 * corrections computed alongside feed the within-round noise track (see the
 * var_noise struct comment).
 */
static void
finalize_round(RoslJoinState *st)
{
	double		base = 0.0;
	double		corr = 0.0;
	double		corrA = 0.0;
	double		corrB = 0.0;
	double		Yhat;
	double		v;
	double		Kn = (double) st->k_count;
	double		pi_repr = 1.0;
	double		round_pairs;
	int			La = st->exp_cache_lim / 2;
	int			Lb = st->exp_cache_lim - La;
	int			c;
	int			t;
	int			n = st->m_count;

	/* AIPW baseline term over ALL of M (cached and not) */
	for (t = 0; t < n; t++)
		base += st->qhat[t] * Kn;

	/*
	 * AIPW correction over the cached (probed) tuples: the full-cache
	 * correction (the score, unchanged) plus the two half-cache corrections
	 * (telemetry only).  A tuple drawn in both halves contributes to both,
	 * each with its own half inclusion probability.
	 */
	for (c = 0; c < st->cache_count; c++)
	{
		int			m = st->cache_midx[c];
		double		pm = st->p[m];
		double		pi = 1.0 - pow(1.0 - pm, (double) st->exp_cache_lim);
		double		mhat = st->qhat[m] * Kn;	/* frozen match-count predictor */
		double		resid = (double) st->round_match[m] - mhat;

		if (pi > 0.0)
		{
			corr += resid / pi;
			if (pi < pi_repr)
				pi_repr = pi;		/* track rarest included tuple           */
		}
		if (st->in_half_a[m])
		{
			double		pia = 1.0 - pow(1.0 - pm, (double) La);

			if (pia > 0.0)
				corrA += resid / pia;
		}
		if (st->in_half_b[m])
		{
			double		pib = 1.0 - pow(1.0 - pm, (double) Lb);

			if (pib > 0.0)
				corrB += resid / pib;
		}
	}

	Yhat = base + corr;				/* the emitted score: full cache, as before */
	/* split-half within-round variance: Y_A - Y_B = corrA - corrB (base cancels) */
	v = 0.25 * (corrA - corrB) * (corrA - corrB);

	round_pairs = (double) st->m_count * Kn;
	accumulate_round(st, round_pairs, Yhat, pi_repr, v);
	record_trajectory(st);
}

/*
 * Finalise a completed EXPLORATION round (one M-block x prefix P).
 *
 * Exploration draws a UNIFORM cache over M, so every selected tuple shares the
 * inclusion probability pi_exp = 1 - (1 - 1/Mn)^L.  Untried tuples have no
 * joinability signal yet, so the exploration round uses the raw HT score (no
 * control variate):
 *
 *   Y_hat_exp = sum over cached t of round_match(t) / pi_exp
 *
 * over region R x P (round_pairs = Mn * p_count).  It is pooled by the same
 * adaptively-weighted accumulate_round(), with pi_repr = pi_exp.
 */
static void
finalize_explore_round(RoslJoinState *st)
{
	double		Mn = (double) st->m_count;
	double		pi_exp;
	double		pia;
	double		pib;
	double		Yhat = 0.0;
	double		YA = 0.0;
	double		YB = 0.0;
	double		v;
	double		round_pairs;
	int			La = st->exp_cache_lim / 2;
	int			Lb = st->exp_cache_lim - La;
	int			c;

	if (Mn <= 0.0 || st->p_count <= 0)
		return;

	pi_exp = 1.0 - pow(1.0 - 1.0 / Mn, (double) st->exp_cache_lim);
	pia = 1.0 - pow(1.0 - 1.0 / Mn, (double) La);
	pib = 1.0 - pow(1.0 - 1.0 / Mn, (double) Lb);

	for (c = 0; c < st->cache_count; c++)
	{
		int			m = st->cache_midx[c];
		double		y = (double) st->round_match[m];

		if (pi_exp > 0.0)
			Yhat += y / pi_exp;
		if (st->in_half_a[m] && pia > 0.0)
			YA += y / pia;
		if (st->in_half_b[m] && pib > 0.0)
			YB += y / pib;
	}
	/* split-half within-round variance (see struct comment) */
	v = 0.25 * (YA - YB) * (YA - YB);

	round_pairs = Mn * (double) st->p_count;
	accumulate_round(st, round_pairs, Yhat, pi_exp, v);
	record_trajectory(st);
}

/*
 * Lazily allocate the ROSL working state the first time enable_rosl is on for
 * this node.  Kept out of ExecInitNestLoop so that stock-path nodes (the
 * common case, including all of initdb) pay nothing.
 */
static void
rosl_state_init(NestLoopState *node)
{
	NestLoop   *nl = (NestLoop *) node->js.ps.plan;
	RoslJoinState *st;
	TupleDesc	outerDesc;
	TupleDesc	innerDesc;
	int			i;

	st = (RoslJoinState *) palloc0(sizeof(RoslJoinState));

	st->m_lim = ROSL_M_LIM;
	st->k_lim = ROSL_K_LIM;
	st->exp_cache_lim = ROSL_EXP_CACHE_LIM;
	st->n_probes = ROSL_N_PROBES;
	st->epsilon_fixed = ROSL_EPSILON;
	st->alpha = ROSL_ALPHA;

	/* known pair count for the extrapolation (planner row estimates) */
	st->num_outer = outerPlan(nl)->plan_rows;
	st->num_inner = innerPlan(nl)->plan_rows;

	/*
	 * Estimate the total number of rounds up front for the two-point
	 * allocation rate.  Each M-block contributes 1 exploration round plus one
	 * exploitation round per body K-block (body = |S| - n_probes).  This need
	 * only be approximate for valid inference (paper: any allocation rate
	 * inside the Theorem-3 bounds is fine); accuracy affects efficiency only.
	 */
	{
		double		n_mblocks = ceil(st->num_outer / (double) st->m_lim);
		double		body = st->num_inner - (double) st->n_probes;
		double		n_kblocks;

		if (body < 0.0)
			body = 0.0;
		n_kblocks = ceil(body / (double) st->k_lim);
		st->t_est = n_mblocks * (1.0 + n_kblocks);
		if (st->t_est < 1.0)
			st->t_est = 1.0;
	}

	outerDesc = ExecGetResultType(outerPlanState(node));
	innerDesc = ExecGetResultType(innerPlanState(node));

	st->m_slots = (TupleTableSlot **) palloc(sizeof(TupleTableSlot *) * st->m_lim);
	for (i = 0; i < st->m_lim; i++)
		st->m_slots[i] = MakeSingleTupleTableSlot(outerDesc);
	st->k_slots = (TupleTableSlot **) palloc(sizeof(TupleTableSlot *) * st->k_lim);
	for (i = 0; i < st->k_lim; i++)
		st->k_slots[i] = MakeSingleTupleTableSlot(innerDesc);

	/* exploration prefix buffer P = S[0 : n_probes] */
	st->p_slots = (TupleTableSlot **) palloc(sizeof(TupleTableSlot *) * st->n_probes);
	for (i = 0; i < st->n_probes; i++)
		st->p_slots[i] = MakeSingleTupleTableSlot(innerDesc);
	st->p_count = 0;

	st->reward = (int *) palloc(sizeof(int) * st->m_lim);
	st->attempts = (int *) palloc(sizeof(int) * st->m_lim);
	st->round_match = (int *) palloc(sizeof(int) * st->m_lim);
	st->p = (double *) palloc(sizeof(double) * st->m_lim);
	st->q = (double *) palloc(sizeof(double) * st->m_lim);
	st->qhat = (double *) palloc(sizeof(double) * st->m_lim);
	st->cum = (double *) palloc(sizeof(double) * st->m_lim);
	st->in_cache = (bool *) palloc(sizeof(bool) * st->m_lim);
	st->in_half_a = (bool *) palloc(sizeof(bool) * st->m_lim);
	st->in_half_b = (bool *) palloc(sizeof(bool) * st->m_lim);
	st->interim = (int *) palloc(sizeof(int) * st->exp_cache_lim);
	st->cache_midx = (int *) palloc(sizeof(int) * st->exp_cache_lim);

	/* per-round trajectory buffers (dumped once at teardown) */
	st->traj_round          = (long *)   palloc(sizeof(long) * ROSL_TRAJ_CAP);
	st->traj_mean_per_pair  = (double *) palloc(sizeof(double) * ROSL_TRAJ_CAP);
	st->traj_est_join       = (double *) palloc(sizeof(double) * ROSL_TRAJ_CAP);
	st->traj_ci_halfwidth   = (double *) palloc(sizeof(double) * ROSL_TRAJ_CAP);
	st->traj_ci_eb          = (double *) palloc(sizeof(double) * ROSL_TRAJ_CAP);
	st->traj_pairs_seen     = (double *) palloc(sizeof(double) * ROSL_TRAJ_CAP);
	st->traj_sample_matches = (long *)   palloc(sizeof(long) * ROSL_TRAJ_CAP);
	st->traj_elapsed_ms     = (double *) palloc(sizeof(double) * ROSL_TRAJ_CAP);
	st->traj_count          = 0;
	st->traj_truncated      = false;

	st->t_started = false;

	/* adaptively-weighted estimator accumulators start empty */
	st->est_num = 0.0;
	st->weight_sum = 0.0;
	st->est_den = 0.0;
	st->mom_yy = 0.0;
	st->mom_yp = 0.0;
	st->mom_pp = 0.0;
	st->stick = 1.0;
	st->round_idx = 0;
	st->eb_sx = st->eb_sxx = st->eb_max = 0.0;
	st->eb_n = 0;
	st->blk_ynum = st->blk_wpair = 0.0;
	st->blk_ya = st->blk_wa = st->blk_yb = st->blk_wb = 0.0;
	st->blk_round = 0;
	st->blk_open = false;
	st->clust_ss = st->split_ss = 0.0;
	st->var_noise = 0.0;
	st->max_h2v = 0.0;
	st->n_blocks = 0;
	st->act_outer = 0.0;			/* exact |R|: accumulated per M-block      */

	st->phase = PH_NEW_MBLOCK;

	/*
	 * Seed the per-state PRNG from a mix of high-resolution time, the backend
	 * PID, and the state pointer, so concurrently launched workers get
	 * independent streams.  (The old srand(time(NULL)) gave same-second workers
	 * identical caches, correlating replicates.)  Force non-zero: xorshift64*
	 * cannot leave state 0.
	 */
	st->rng_state = (uint64) GetCurrentTimestamp()
		^ ((uint64) MyProcPid << 32)
		^ (uint64) (uintptr_t) st;
	if (st->rng_state == 0)
		st->rng_state = UINT64CONST(0x9E3779B97F4A7C15);

	node->rosl = (void *) st;
}


/* ----------------------------------------------------------------
 *		ExecStockNestLoop  --  exact PostgreSQL nested loop (default path)
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecStockNestLoop(NestLoopState *node)
{
	NestLoop   *nl = (NestLoop *) node->js.ps.plan;
	PlanState  *outerPlan = outerPlanState(node);
	PlanState  *innerPlan = innerPlanState(node);
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState  *joinqual = node->js.joinqual;
	ExprState  *otherqual = node->js.ps.qual;
	ExprContext *econtext = node->js.ps.ps_ExprContext;
	ListCell   *lc;

	/*
	 * Reset per-tuple memory context to free any expression-evaluation
	 * storage allocated in the previous cycle.
	 */
	ResetExprContext(econtext);

	for (;;)
	{
		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan (and push down any nestloop params).
		 */
		if (node->nl_NeedNewOuter)
		{
			outerTupleSlot = ExecProcNode(outerPlan);

			if (TupIsNull(outerTupleSlot))
				return NULL;		/* end of join */

			econtext->ecxt_outertuple = outerTupleSlot;
			node->nl_NeedNewOuter = false;
			node->nl_MatchedOuter = false;

			foreach(lc, nl->nestParams)
			{
				NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
				int			paramno = nlp->paramno;
				ParamExecData *prm;

				prm = &(econtext->ecxt_param_exec_vals[paramno]);
				Assert(IsA(nlp->paramval, Var));
				prm->value = slot_getattr(outerTupleSlot,
										  nlp->paramval->varattno,
										  &(prm->isnull));
				innerPlan->chgParam = bms_add_member(innerPlan->chgParam,
													 paramno);
			}

			ExecReScan(innerPlan);
		}

		/* get the next inner tuple */
		innerTupleSlot = ExecProcNode(innerPlan);
		econtext->ecxt_innertuple = innerTupleSlot;

		if (TupIsNull(innerTupleSlot))
		{
			node->nl_NeedNewOuter = true;

			if (!node->nl_MatchedOuter &&
				(node->js.jointype == JOIN_LEFT ||
				 node->js.jointype == JOIN_ANTI))
			{
				/* emit outer + null-extended inner */
				econtext->ecxt_innertuple = node->nl_NullInnerTupleSlot;

				if (otherqual == NULL || ExecQual(otherqual, econtext))
					return ExecProject(node->js.ps.ps_ProjInfo);
				else
					InstrCountFiltered2(node, 1);
			}
			continue;
		}

		if (ExecQual(joinqual, econtext))
		{
			node->nl_MatchedOuter = true;

			/* antijoin: this outer is matched, so move on */
			if (node->js.jointype == JOIN_ANTI)
			{
				node->nl_NeedNewOuter = true;
				continue;
			}

			/* at most one match wanted? */
			if (node->js.single_match)
				node->nl_NeedNewOuter = true;

			if (otherqual == NULL || ExecQual(otherqual, econtext))
				return ExecProject(node->js.ps.ps_ProjInfo);
			else
				InstrCountFiltered2(node, 1);
		}
		else
			InstrCountFiltered1(node, 1);

		ResetExprContext(econtext);
	}
}


/* ----------------------------------------------------------------
 *		ExecRoslNestLoop  --  streaming ROSL sampling join with HT estimator
 *
 *		Single-phase driver.  Consumes R in M-blocks; for each M-block scans
 *		all of S in K-blocks.  Each round draws a joinability-weighted exploit cache
 *		and probes it against the current K-block, emitting matching rows one
 *		per call.  Resume cursors (probe_ci, probe_kj) track position within a
 *		round across calls.  At round end, finalize_round() folds the
 *		Horvitz-Thompson estimate and epsilon is halved.  node->rosl is
 *		guaranteed non-NULL by the dispatcher.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecRoslNestLoop(NestLoopState *node)
{
	RoslJoinState *st = (RoslJoinState *) node->rosl;
	PlanState  *outerPlan = outerPlanState(node);
	PlanState  *innerPlan = innerPlanState(node);
	ExprState  *joinqual = node->js.joinqual;
	ExprState  *otherqual = node->js.ps.qual;
	ExprContext *econtext = node->js.ps.ps_ExprContext;

	ResetExprContext(econtext);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		switch (st->phase)
		{
			case PH_NEW_MBLOCK:
				{
					/*
					 * Close the M-block that just finished (if any): fold its
					 * clustered + split-half residuals into the running SS.
					 * Rounds within a block share adaptive feedback, so the
					 * block is the independence unit for those CIs.
					 */
					close_block(st);

					st->m_count = load_outer_block(st, outerPlan);
					if (st->m_count == 0)
					{
						/*
						 * Outer exhausted: no further M-blocks, so no further
						 * rounds will ever run.  We do NOT emit anything here:
						 * matching Saketh's model, the only mid-stream signal
						 * the client gets is the cursor returning NULL.  The
						 * full estimate + trajectory is dumped once at teardown
						 * (PrintRoslCounters in ExecEndNestLoop).  Completion is
						 * therefore detected client-side purely by fetchone()
						 * returning None -- no NOTICE parsing, no deadlock.
						 */
						st->phase = PH_DONE;
						break;
					}

					/* Exact outer cardinality |R|: sum m_count over M-blocks */
					st->act_outer += (double) st->m_count;

					/*
					 * Anchor per-round timing at the first non-empty M-block,
					 * i.e. the moment real work begins.  Measured in C so it is
					 * independent of when the client fetches or libpq flushes.
					 */
					if (!st->t_started)
					{
						st->t_start = GetCurrentTimestamp();
						st->t_started = true;
					}

					/*
					 * Fresh M-block: zero rewards & attempts (r(t), a(t) are
					 * per-M-block).  Rewind S and load the exploration prefix P
					 * = S[0 : n_probes] first; the body scan later resumes from
					 * tuple n_probes+1, so P and the body are disjoint.
					 * Epsilon is FIXED (epsilon_fixed) in this version, so there
					 * is nothing to reset per M-block.
					 */
					memset(st->reward, 0, sizeof(int) * st->m_count);
					memset(st->attempts, 0, sizeof(int) * st->m_count);
					ExecReScan(innerPlan);
					st->p_count = load_prefix_block(st, innerPlan);

					st->phase = PH_EXPLORE;
					break;
				}

			case PH_EXPLORE:
				{
					int			ci;
					int			pj;

					/*
					 * Probabilistic exploration round for this M-block: draw a
					 * UNIFORM cache over M (once) and probe it against the whole
					 * prefix P (region R x P).  If P is empty (n_probes larger
					 * than |S|, or an empty inner), skip straight to the body.
					 */
					if (st->p_count == 0)
					{
						st->phase = PH_NEW_SBLOCK;
						break;
					}

					if (!st->explore_built)
					{
						/* first entry for this M-block: build the round once */
						build_distribution(st, true /* uniform */);
						build_cumulative(st);
						draw_and_dedup_cache(st);
						memset(st->round_match, 0, sizeof(int) * st->m_count);

						/*
						 * a(t) += n_probes for every uniquely cached tuple:
						 * each is tested against all of P.  Done up front so
						 * a(t) is correct regardless of how the resume cursors
						 * slice the probing.
						 */
						{
							int			c;

							for (c = 0; c < st->cache_count; c++)
								st->attempts[st->cache_midx[c]] += st->p_count;
						}

						st->explore_built = true;
						st->explore_ci = 0;
						st->explore_pj = 0;
					}

					ci = st->explore_ci;
					pj = st->explore_pj;

					while (ci < st->cache_count)
					{
						int			m = st->cache_midx[ci];
						TupleTableSlot *outerSlot = st->m_slots[m];

						while (pj < st->p_count)
						{
							TupleTableSlot *innerSlot = st->p_slots[pj];

							CHECK_FOR_INTERRUPTS();

							econtext->ecxt_outertuple = outerSlot;
							econtext->ecxt_innertuple = innerSlot;
							pj++;
							st->t_steps++;

							if (ExecQual(joinqual, econtext))
							{
								if (otherqual == NULL ||
									ExecQual(otherqual, econtext))
								{
									st->reward[m]++;
									st->round_match[m]++;
									st->sample_matches++;
									st->explore_ci = ci;
									st->explore_pj = pj;
									return ExecProject(node->js.ps.ps_ProjInfo);
								}
								else
									InstrCountFiltered2(node, 1);
							}
							else
								InstrCountFiltered1(node, 1);

							ResetExprContext(econtext);
						}
						pj = 0;
						ci++;
					}

					/* Exploration round complete: fold its HT score (R x P) */
					finalize_explore_round(st);

					st->explore_ci = 0;
					st->explore_pj = 0;
					st->explore_built = false;
					st->phase = PH_NEW_SBLOCK;
					break;
				}

			case PH_NEW_SBLOCK:
				{
					st->k_count = load_inner_block(st, innerPlan);
					if (st->k_count == 0)
					{
						/* S exhausted for this M-block: advance outer */
						st->phase = PH_NEW_MBLOCK;
						break;
					}

					build_distribution(st, false /* eps-greedy on joinability */);
					build_cumulative(st);		/* cum[]                   */
					draw_and_dedup_cache(st);	/* -> cache_midx[]         */
					memset(st->round_match, 0, sizeof(int) * st->m_count);

					/*
					 * Account join attempts for this round up front: every
					 * unique cached tuple is probed against all k_count tuples
					 * in the current K-block, so a(t) += |K| for each.  Doing
					 * this here (rather than inside the resumable probe loop)
					 * keeps a(t) correct regardless of how the per-call resume
					 * cursors slice the probing, and matches the pseudocode's
					 * "a(texp M) += |K|" before the inner K scan.
					 */
					{
						int		c;

						for (c = 0; c < st->cache_count; c++)
							st->attempts[st->cache_midx[c]] += st->k_count;
					}

					st->probe_ci = 0;			/* start cache scan from beginning */
					st->probe_kj = 0;			/* start K-block scan from beginning */
					st->phase = PH_PROBE;
					break;
				}

			case PH_PROBE:
				{
					int			ci = st->probe_ci;
					int			kj = st->probe_kj;

					/*
					 * Probabilistic round: probe the exploit cache against the
					 * K-block, emitting each matching row immediately.  Resume
					 * cursors (probe_ci, probe_kj) let the state machine pick up
					 * exactly where it left off after each returned row.  Rewards
					 * and per-round match counts are accumulated here so that
					 * finalize_round() has everything it needs when the last pair
					 * in the round has been probed.
					 */
					while (ci < st->cache_count)
					{
						int			m = st->cache_midx[ci];
						TupleTableSlot *outerSlot = st->m_slots[m];

						while (kj < st->k_count)
						{
							TupleTableSlot *innerSlot = st->k_slots[kj];

							CHECK_FOR_INTERRUPTS();

							econtext->ecxt_outertuple = outerSlot;
							econtext->ecxt_innertuple = innerSlot;
							kj++;		/* advance now; resume picks up here */
							st->t_steps++;

							if (ExecQual(joinqual, econtext))
							{
								if (otherqual == NULL ||
									ExecQual(otherqual, econtext))
								{
									st->reward[m]++;		/* selection feedback  */
									st->round_match[m]++;	/* estimator numerator */
									st->sample_matches++;	/* diagnostics only    */
									st->probe_ci = ci;		/* save resume position */
									st->probe_kj = kj;
									return ExecProject(node->js.ps.ps_ProjInfo);
								}
								else
									InstrCountFiltered2(node, 1);
							}
							else
								InstrCountFiltered1(node, 1);

							ResetExprContext(econtext);
						}
						kj = 0;
						ci++;
					}

					/*
					 * Round complete: fold the AIPW score into the
					 * adaptively-weighted estimate and emit the trajectory row
					 * (with CI).  Epsilon is FIXED in this version -- no decay.
					 */
					finalize_round(st);

					st->probe_ci = 0;
					st->probe_kj = 0;
					st->phase = PH_NEW_SBLOCK;
					break;
				}

			case PH_DONE:
			default:
				return NULL;
		}
	}
}


/* ----------------------------------------------------------------
 *		ExecNestLoop  --  dispatch on the enable_rosl GUC
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecNestLoop(PlanState *pstate)
{
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop   *nl = (NestLoop *) node->js.ps.plan;

	CHECK_FOR_INTERRUPTS();

	/*
	 * Plan-shape guard.  The ROSL path assumes a plain INNER join with a
	 * NON-parameterised inner: it rescans the whole inner per M-block (valid
	 * only for a fixed, non-parameterised inner) and emits inner-join matches
	 * only.  If the planner picked a parameterised nestloop (nestParams != NIL)
	 * or a non-inner join, running ROSL would silently produce wrong results,
	 * so fall back to the exact stock nested loop even when the GUC is on.
	 * (The experimental 2-table inner-join queries this estimator targets
	 * always satisfy this.)
	 */
	if (!enable_rosl ||
		nl->nestParams != NIL ||
		node->js.jointype != JOIN_INNER)
		return ExecStockNestLoop(node);

	if (node->rosl == NULL)
		rosl_state_init(node);

	return ExecRoslNestLoop(node);
}


/* ----------------------------------------------------------------
 *		ExecInitNestLoop
 * ----------------------------------------------------------------
 */
NestLoopState *
ExecInitNestLoop(NestLoop *node, EState *estate, int eflags)
{
	NestLoopState *nlstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	NL1_printf("ExecInitNestLoop: %s\n", "initializing node");

	/* create state structure */
	nlstate = makeNode(NestLoopState);
	nlstate->js.ps.plan = (Plan *) node;
	nlstate->js.ps.state = estate;
	nlstate->js.ps.ExecProcNode = ExecNestLoop;

	/* expression context */
	ExecAssignExprContext(estate, &nlstate->js.ps);

	/* initialize child nodes */
	outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
	if (node->nestParams == NIL)
		eflags |= EXEC_FLAG_REWIND;
	else
		eflags &= ~EXEC_FLAG_REWIND;
	innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate, eflags);

	/* result slot, type and projection */
	ExecInitResultTupleSlotTL(estate, &nlstate->js.ps);
	ExecAssignProjectionInfo(&nlstate->js.ps, NULL);

	/* child expressions */
	nlstate->js.ps.qual =
		ExecInitQual(node->join.plan.qual, (PlanState *) nlstate);
	nlstate->js.jointype = node->join.jointype;
	nlstate->js.joinqual =
		ExecInitQual(node->join.joinqual, (PlanState *) nlstate);

	nlstate->js.single_match = (node->join.inner_unique ||
								node->join.jointype == JOIN_SEMI);

	/* null tuples for outer joins, if needed */
	switch (node->join.jointype)
	{
		case JOIN_INNER:
		case JOIN_SEMI:
			break;
		case JOIN_LEFT:
		case JOIN_ANTI:
			nlstate->nl_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
									  ExecGetResultType(innerPlanState(nlstate)));
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}

	nlstate->nl_NeedNewOuter = true;
	nlstate->nl_MatchedOuter = false;

	/*
	 * ROSL working state is allocated lazily (only if enable_rosl is on when
	 * this node first executes), so the stock path costs nothing extra.
	 */
	nlstate->rosl = NULL;

	NL1_printf("ExecInitNestLoop: %s\n", "node initialized");

	return nlstate;
}


/* ----------------------------------------------------------------
 *		PrintRoslCounters  --  dump the full ROSL trajectory + summary
 *
 *		The single point of measurement communication, called once from
 *		ExecEndNestLoop at executor teardown.  Mirrors Saketh's
 *		PrintNodeCounters: everything goes to the server log via elog(INFO),
 *		nothing to the client mid-stream.  A test harness recovers accuracy
 *		and timing by parsing the *server log* after the cursor has drained,
 *		never from the row/notice stream during the join.
 *
 *		Two record kinds are emitted, both prefixed so they are easy to grep:
 *		  ROSL_TRAJ  -- one line per round (round, mu, est_join, pairs_seen,
 *		                sample_matches, elapsed_ms)
 *		  ROSL_SUMM  -- one final summary line (final est_join, total rounds,
 *		                total sample_matches, total pairs_seen, t_steps)
 * ----------------------------------------------------------------
 */
static void
PrintRoslCounters(RoslJoinState *st)
{
	int			i;

	if (st == NULL)
		return;

	/* per-round trajectory: one INFO line per recorded round */
	for (i = 0; i < st->traj_count; i++)
	{
		elog(INFO,
			 "ROSL_TRAJ round=%ld mean_per_pair=%.10f est_join=%.2f "
			 "ci_halfwidth=%.2f pairs_seen=%.0f sample_matches=%ld "
			 "elapsed_ms=%.3f ci_eb=%.2f",
			 st->traj_round[i],
			 st->traj_mean_per_pair[i],
			 st->traj_est_join[i],
			 st->traj_ci_halfwidth[i],
			 st->traj_pairs_seen[i],
			 st->traj_sample_matches[i],
			 st->traj_elapsed_ms[i],
			 st->traj_ci_eb[i]);
	}

	if (st->traj_truncated)
		elog(INFO,
			 "ROSL_TRAJ truncated: more than %d rounds; trajectory capped "
			 "(use a smaller scale factor)", ROSL_TRAJ_CAP);

	/*
	 * Final summary.  The point estimate uses the WEIGHTED denominator
	 * weight_sum (adaptively-weighted mean), not the raw pair count est_den.
	 * The extrapolation now uses the EXACT outer count act_outer (summed over
	 * every M-block, so it is exact once the scan has finished) instead of the
	 * planner's plan_rows -- the sampler only estimates a per-pair rate, and the
	 * outer population size is known exactly by the time we get here.  The inner
	 * size is still the planner estimate num_inner (this version rescans S per
	 * M-block rather than materialising it, so it never counts |S| exactly; see
	 * note in the header).  A final self-normalized CI half-width is recomputed
	 * here from the moment accumulators (Konig-Huygens), matching the per-round
	 * trajectory.  The `final_est_join=` token is kept verbatim for the parser.
	 */
	/*
	 * Fold the final (still-open) M-block into the block-level SS before the
	 * clustered / split-half CIs are read.  Idempotent if already closed.
	 */
	close_block(st);

	if (st->weight_sum > 0.0)
	{
		double		mu = st->est_num / st->weight_sum;
		double		pop = st->act_outer * st->num_inner;
		double		est_join = mu * pop;
		double		ss = st->mom_yy - 2.0 * mu * st->mom_yp
			+ mu * mu * st->mom_pp;
		double		Vhat;
		double		ci_half;
		double		ci_clust;
		double		ci_split;
		double		ci_eb;
		double		est_eb;

		if (ss < 0.0)
			ss = 0.0;
		Vhat = ss / (st->weight_sum * st->weight_sum);
		ci_half = 1.96 * pop * sqrt(Vhat);

		/*
		 * M-block-clustered CI (Section 7.h).  df = G-1 with a G/(G-1)
		 * small-sample scale on the between-block SS; falls back to the
		 * round-level half-width when fewer than two blocks are available
		 * (a single block's residual is degenerately ~0, mu having been fit
		 * from that same block).
		 */
		if (st->n_blocks >= 2)
		{
			double		g = (double) st->n_blocks;
			double		ss_c = st->clust_ss * g / (g - 1.0);

			ci_clust = t_crit_975(st->n_blocks - 1) * pop
				* sqrt(ss_c) / st->weight_sum;
		}
		else
			ci_clust = ci_half;

		/*
		 * Split-half within-block CI (Section 7.h).  Heterogeneity-free floor;
		 * df = number of blocks that contributed both halves.  Falls back to
		 * the round-level half-width if none did.
		 */
		if (st->n_blocks >= 1 && st->split_ss > 0.0)
			ci_split = t_crit_975(st->n_blocks) * pop
				* sqrt(st->split_ss) / st->weight_sum;
		else
			ci_split = ci_half;

		/*
		 * Empirical-Bernstein confidence sequence (Section 8 guard band) on
		 * the bounded per-round rate X = Yhat/pairs.  Maurer-Pontil form:
		 *   |Xbar - mu| <= sqrt(2 Vx ln(2/alpha) / N) + (7/3) B ln(2/alpha)/N.
		 * est_eb centres on Xbar (the unweighted per-round mean), which the
		 * bound is stated for; the weighted mu above remains the primary point
		 * estimate.  This interval needs no variance convergence and is the
		 * only one here valid at a data-dependent stop (output LIMIT).
		 */
		if (st->eb_n > 1)
		{
			double		xb = st->eb_sx / (double) st->eb_n;
			double		vx = st->eb_sxx / (double) st->eb_n - xb * xb;
			double		lg = 3.6888794541139363;	/* ln(2/0.05) */
			double		half_rate;

			if (vx < 0.0)
				vx = 0.0;
			half_rate = sqrt(2.0 * vx * lg / (double) st->eb_n)
				+ (7.0 / 3.0) * st->eb_max * lg / (double) st->eb_n;
			ci_eb = half_rate * pop;
			est_eb = xb * pop;
		}
		else
		{
			ci_eb = ci_half;
			est_eb = est_join;
		}

		/*
		 * Noise-only interval from the split-cache within-round variance,
		 * plus the decomposition diagnostic the clustered CI needs:
		 *   het_ratio = clustered SS / var_noise  --  ~1 means the clustered
		 * residuals are mostly estimator noise (clustered CI trustworthy);
		 * >>1 means cross-block truth heterogeneity dominates (clustered CI
		 * over-wide by roughly that factor).  lindeberg_max certifies no
		 * single round dominates the noise (Fix-3 telemetry).
		 */
		{
			double		ci_noise = 1.96 * pop
				* sqrt(st->var_noise > 0.0 ? st->var_noise : 0.0)
				/ st->weight_sum;
			double		het_ratio = -1.0;	/* -1: undefined (var_noise==0) */

			if (st->var_noise > 0.0 && st->n_blocks >= 2)
			{
				double		g = (double) st->n_blocks;

				het_ratio = (st->clust_ss * g / (g - 1.0)) / st->var_noise;
			}

			elog(INFO,
				 "ROSL_SUMM final_est_join=%.2f ci_halfwidth=%.2f "
				 "ci_clust=%.2f ci_split=%.2f ci_eb=%.2f est_eb=%.2f "
				 "ci_noise=%.2f het_ratio=%.2f lindeberg_max=%.3e "
				 "n_blocks=%ld "
				 "rounds=%ld sample_matches=%ld pairs_seen=%.0f t_steps=%ld "
				 "act_outer=%.0f num_outer=%.0f num_inner=%.0f",
				 est_join, ci_half, ci_clust, ci_split, ci_eb, est_eb,
				 ci_noise, het_ratio, st->max_h2v,
				 st->n_blocks, st->rounds, st->sample_matches, st->est_den,
				 st->t_steps, st->act_outer, st->num_outer, st->num_inner);
		}
	}
	else
		elog(INFO,
			 "ROSL_SUMM final_est_join=0.00 rounds=0 sample_matches=%ld "
			 "(no completed rounds: outer empty or join produced no pairs)",
			 st->sample_matches);
}


/* ----------------------------------------------------------------
 *		ExecEndNestLoop
 * ----------------------------------------------------------------
 */
void
ExecEndNestLoop(NestLoopState *node)
{
	RoslJoinState *st = (RoslJoinState *) node->rosl;

	NL1_printf("ExecEndNestLoop: %s\n", "ending node processing");

	/* free the exprcontext */
	ExecFreeExprContext(&node->js.ps);

	/* clean out the tuple table */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

	/* tear down ROSL state if it was ever allocated */
	if (st != NULL)
	{
		int			i;

		/*
		 * Single point of measurement communication: dump the full per-round
		 * trajectory and final summary to the server log, once, at teardown.
		 * Nothing was emitted to the client during the join (Saketh model).
		 */
		PrintRoslCounters(st);

		for (i = 0; i < st->m_lim; i++)
			if (!TupIsNull(st->m_slots[i]))
				ExecDropSingleTupleTableSlot(st->m_slots[i]);
		for (i = 0; i < st->k_lim; i++)
			if (!TupIsNull(st->k_slots[i]))
				ExecDropSingleTupleTableSlot(st->k_slots[i]);
		for (i = 0; i < st->n_probes; i++)
			if (!TupIsNull(st->p_slots[i]))
				ExecDropSingleTupleTableSlot(st->p_slots[i]);

		pfree(st->m_slots);
		pfree(st->k_slots);
		pfree(st->p_slots);
		pfree(st->reward);
		pfree(st->attempts);
		pfree(st->round_match);
		pfree(st->p);
		pfree(st->q);
		pfree(st->qhat);
		pfree(st->cum);
		pfree(st->in_cache);
		pfree(st->in_half_a);
		pfree(st->in_half_b);
		pfree(st->interim);
		pfree(st->cache_midx);
		pfree(st->traj_round);
		pfree(st->traj_mean_per_pair);
		pfree(st->traj_est_join);
		pfree(st->traj_ci_halfwidth);
		pfree(st->traj_ci_eb);
		pfree(st->traj_pairs_seen);
		pfree(st->traj_sample_matches);
		pfree(st->traj_elapsed_ms);
		pfree(st);
		node->rosl = NULL;
	}

	/* close down subplans */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));

	NL1_printf("ExecEndNestLoop: %s\n", "node processing ended");
}


/* ----------------------------------------------------------------
 *		ExecReScanNestLoop
 * ----------------------------------------------------------------
 */
void
ExecReScanNestLoop(NestLoopState *node)
{
	PlanState  *outerPlan = outerPlanState(node);
	RoslJoinState *st = (RoslJoinState *) node->rosl;

	/*
	 * If outerPlan->chgParam is not null then the plan will be automatically
	 * re-scanned by the first ExecProcNode.
	 */
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);

	/* stock-path bookkeeping */
	node->nl_NeedNewOuter = true;
	node->nl_MatchedOuter = false;

	/* reset the ROSL state machine + running estimator, if allocated */
	if (st != NULL)
	{
		st->phase = PH_NEW_MBLOCK;
		st->m_count = 0;
		st->k_count = 0;
		st->p_count = 0;
		st->cache_count = 0;
		st->probe_ci = 0;
		st->probe_kj = 0;
		st->explore_ci = 0;
		st->explore_pj = 0;
		st->explore_built = false;

		/* adaptively-weighted estimator: clear all accumulators */
		st->est_num = 0.0;
		st->weight_sum = 0.0;
		st->est_den = 0.0;
		st->mom_yy = 0.0;
		st->mom_yp = 0.0;
		st->mom_pp = 0.0;
		st->stick = 1.0;
		st->round_idx = 0;
		st->eb_sx = st->eb_sxx = st->eb_max = 0.0;
		st->eb_n = 0;
		st->blk_ynum = st->blk_wpair = 0.0;
		st->blk_ya = st->blk_wa = st->blk_yb = st->blk_wb = 0.0;
		st->blk_round = 0;
		st->blk_open = false;
		st->clust_ss = st->split_ss = 0.0;
		st->var_noise = 0.0;
		st->max_h2v = 0.0;
		st->n_blocks = 0;
		st->act_outer = 0.0;			/* exact |R| accumulator restarts       */

		st->rounds = 0;
		st->t_steps = 0;
		st->sample_matches = 0;

		/*
		 * Advance the PRNG to a fresh stream for the re-scan, so a node executed
		 * multiple times (e.g. under a rescanning parent) does not replay the
		 * identical sequence of caches each pass.
		 */
		st->rng_state ^= (uint64) GetCurrentTimestamp()
			^ ((uint64) MyProcPid << 17);
		if (st->rng_state == 0)
			st->rng_state = UINT64CONST(0x9E3779B97F4A7C15);

		/* fresh run: discard prior trajectory and re-anchor timing */
		st->traj_count = 0;
		st->traj_truncated = false;
		st->t_started = false;
	}
}