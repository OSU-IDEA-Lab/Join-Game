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
 * VARIANT IMPLEMENTED HERE: Deterministic Exploration with Fixed Probes,
 * global without-replacement, TWO-REGION estimate (progress doc section 5).
 * This is the corrected Fixed-Probe baseline (see CORRECTIONS below).
 *
 *   SHARED PREFIX.  Before any M-block, all of S is materialised once and its
 *   first ROSL_N_PROBES tuples are designated the exploration prefix P.  Since
 *   the relation is shuffled at rest, P is a uniform random sample of S.  P is
 *   reused by every M-block and is GLOBALLY excluded from exploitation: it
 *   never appears in any K-block, so no (outer,inner) pair is ever probed in
 *   both phases.  This partitions R x S into two disjoint regions: the
 *   exploration region R x P and the exploitation body R x B, where B = S \ P
 *   and |B| = |S| - n_probes.
 *
 *   1. EXPLORATION (deterministic; region R x P).  Every tuple t in the
 *      M-block is probed against all ROSL_N_PROBES prefix tuples.  r(t) counts
 *      how many joined; a(t) is the design constant n_probes for every tuple,
 *      so q(t) = r(t)/n_probes is uncensored with no a(t)=0 edge case.  These
 *      pairs are observed with probability 1 (certainty units): every match is
 *      counted EXACTLY into expl_exact and emitted as join output; it is never
 *      inverse-probability weighted.  Because M-blocks tile R and each t is
 *      probed against all of P, the region R x P is observed in full.
 *
 *   2. EXPLOITATION (running estimate; region R x B).  The body B is scanned in
 *      K-blocks of ROSL_K_LIM tuples (the prefix is skipped).  Each (M-block,
 *      K-block) round draws an exploit cache of unique outer tuples from a
 *      distribution that is epsilon-greedy on the FROZEN joinability q(t),
 *      probes it against the body K-block, emits matches, and folds a
 *      Horvitz-Thompson success estimate into the running body estimate.  q(t)
 *      and epsilon are fixed for the whole M-block, so the per-round inclusion
 *      probabilities depend only on the exploration probes against P.
 *
 *   TWO-REGION ESTIMATE.  R x P is known exactly; only R x B is estimated:
 *      mu_hat_B = est_num / est_den            (HT mean successes per BODY pair)
 *      J_hat    = expl_exact + mu_hat_B * |R| * |B|        (|B| = |S| - n_probes)
 *   Pairing a body-only denominator (est_den sums |M|*|K| over body blocks)
 *   with a body-only multiplier (|B|) keeps the estimate unbiased; removing
 *   R x P from the estimated population also lowers variance, since those
 *   n_probes*|R| pairs contribute exact outcomes and no estimator noise.  Both
 *   est_num and est_den are fixed by the block schedule (est_den deterministic),
 *   so mu_hat_B is exactly mean-unbiased -- no ratio bias from early stopping.
 *
 * Matching pairs from both phases are emitted immediately (one per call via
 * resume cursors).  Only sampled rows are emitted; this is an approximate join
 * intended for cardinality estimation workloads.
 *
 * ============================ CORRECTIONS ============================
 * This file fixes three weaknesses of the original Fixed-Probe draft:
 *
 *  (1) RNG.  Uses a per-state xorshift64* PRNG seeded from a mix of high-
 *      resolution time, the backend PID, and the state pointer -- NOT process-
 *      global rand()/srand(time()).  The old 1-second srand() seed handed
 *      same-second workers (the concurrent harness launches many) IDENTICAL
 *      "random" caches, correlating runs meant to be independent replicates and
 *      biasing exactly the cross-run variance the experiment measures.
 *
 *  (2) PLAN-SHAPE GUARD.  ExecNestLoop falls back to the exact stock path
 *      unless the join is a plain INNER join with a non-parameterised inner
 *      (nestParams == NIL).  The ROSL path materialises S once and reuses it
 *      (valid only for a fixed inner) and emits inner-join matches only, so
 *      without the guard a parameterised nestloop or outer join chosen by the
 *      planner would silently produce wrong results while the GUC is on.
 *
 *  (3) ACTUAL-COUNT POPULATIONS.  The extrapolation uses the ACTUAL table sizes
 *      -- |S| = s_count (the relation is fully materialised, so this is exact)
 *      and, for the final estimate, |R| = act_outer (every outer tuple is
 *      consumed in M-blocks, so the running sum is exact) -- instead of the
 *      planner's plan_rows.  The sampler only estimates a per-PAIR rate; the
 *      population sizes are known exactly and need not be guessed.  This:
 *        - removes the negative-body cliff: |B| = s_count - pfx >= 0 by
 *          construction (the old plan|S| - n_probes could go negative and was
 *          clamped to 0, silently dropping the whole body term);
 *        - removes inner-relation stats staleness (lineitem is the big inner
 *          here, and its plan_rows is the most error-prone);
 *        - makes the FINAL estimate exactly unbiased given the true sizes:
 *          E[J_hat] = |R join P| + mu_B * act|R| * act|B| = |R join S|.
 *      The running TRAJECTORY still uses plan|R| as a proxy for total outer
 *      cardinality (which is not known until the scan finishes) but actual |S|;
 *      the ROSL_SUMM line reports the authoritative actual-count final estimate.
 *      (Note: CREATE TABLE AS sets reltuples, so plan|R| is usually accurate
 *      anyway; this change makes the final independent of that.)
 * ====================================================================
 *
 * INTEGRATION (unchanged):
 * - execnodes.h: add `void *rosl;` to struct NestLoopState.
 * - guc.c: register the bool GUC `enable_rosl` (see notes at end of file).
 * - nodeNestloop.h: unchanged.
 *
 * ROSL ALGORITHM NOTES:
 * R (outer) is consumed in M-blocks of ROSL_M_LIM tuples.  All of S (inner) is
 * materialised ONCE (on the first M-block) into an in-memory buffer; the same
 * buffer -- and the same prefix P -- serves every M-block.  Exploration probes
 * the fixed prefix s_slots[0 .. n_probes); exploitation walks the body
 * s_slots[n_probes .. s_count) in contiguous K-blocks.  Cache fill is one
 * cumulative-probability pass plus a binary search per draw; deduplication is a
 * sort of the drawn indices and a single linear pass.  Resume cursors let each
 * phase yield one row per call.  Targets INNER-join cardinality with a
 * non-parameterised inner (enforced by the dispatch guard).
 *
 * ASSUMPTION: the inner relation is shuffled at rest, so the first n_probes
 * tuples of the scan are a uniform random sample.  If an order-imposing node
 * (Sort, index scan) sits between the base table and this one, that prefix is
 * not random and q(t) -- and thus the exploit selection -- silently skews.
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

/* ---- ROSL tunables (block sizes, exploration budget, cache budget) ---- */
#define ROSL_M_LIM			1588	/* outer (R) tuples per M-block          */
#define ROSL_K_LIM			1588	/* inner (S) tuples per K-block          */
#define ROSL_EXP_CACHE_LIM	529		/* with-replacement draws per round (L)  */
#define ROSL_N_PROBES		100		/* shared exploration prefix size (a(t))  */
#define ROSL_EPSILON		0.2		/* fixed exploration mass (epsilon)      */

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
	PH_NEW_MBLOCK,					/* load next outer block, materialise S      */
	PH_EXPLORE,						/* fixed random probes per M-tuple           */
	PH_NEW_SBLOCK,					/* take next K-block, draw exploit cache     */
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
	int			n_probes;			/* shared exploration prefix size (a(t)) */

	/* outer (R) block buffer */
	TupleTableSlot **m_slots;		/* [m_lim] copies of current M-block      */
	int			m_count;			/* tuples in current M-block (Mn)         */
	int		   *reward;				/* [m_lim] exploration successes r(t)     */

	/*
	 * inner (S) materialised in full ONCE for the whole run.  The first
	 * n_probes tuples are the shared exploration prefix P (s_slots[0..n_probes));
	 * the body B = s_slots[n_probes..s_count) is what exploitation scans in
	 * contiguous K-blocks [k_start .. k_start+k_count).  s_cap is the allocated
	 * capacity; the buffer grows geometrically as S is read.  s_built records
	 * that materialisation has happened, so it is not repeated per M-block.
	 */
	TupleTableSlot **s_slots;		/* [s_cap] all inner tuples (shared)       */
	int			s_count;			/* number of inner tuples materialised     */
	int			s_cap;				/* allocated capacity of s_slots           */
	bool		s_built;			/* whether S has been materialised yet     */
	int			k_start;			/* offset of current body K-block within S */
	int			k_count;			/* tuples in current K-block (Kn)          */

	/* per-round selection distribution and exploit cache */
	double	   *p;					/* [m_lim] single-draw prob (normalised)  */
	double	   *cum;				/* [m_lim] cumulative of p                */
	int		   *interim;			/* [exp_cache_lim] drawn M-indices (dups) */
	int		   *cache_midx;			/* [exp_cache_lim] unique M-indices       */
	int			cache_count;		/* number of unique cached tuples         */
	int		   *round_match;		/* [m_lim] matches this round, by M-index */

	double		epsilon;			/* fixed exploration mass (hyperparameter)*/

	/* estimator accumulators (persist across all blocks) */
	double		est_num;			/* sum over body rounds of Y_hat_round    */
	double		est_den;			/* sum over body rounds of Mn*Kn (body)   */
	double		expl_exact;			/* EXACT matches in R x P (certainty units)*/
	double		num_outer;			/* |R| planner estimate (trajectory proxy) */
	double		num_inner;			/* |S| planner estimate (reported only)    */
	double		act_outer;			/* |R| ACTUAL: running sum of m_count      */

	/* PH_EXPLORE resume cursors (persist across calls within exploration) */
	int			expl_mi;			/* M-index currently being explored          */
	int			expl_pi;			/* probe number within that M-tuple          */

	/* PH_PROBE resume cursors (persist across calls within one round) */
	int			probe_ci;			/* cache index to resume from                */
	int			probe_kj;			/* K-block index to resume from              */

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
 * clock drew identical caches, correlating runs meant to be independent.
 * Seeded once per state in rosl_state_init from time x PID x pointer; the state
 * is kept non-zero there (xorshift cannot recover from an all-zero state).
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

/*
 * Materialise ALL of S into st->s_slots for the current M-block.  The buffer
 * grows geometrically; each tuple is deep-copied into its own slot so it
 * survives across the many ExecProcNode calls of later phases.  Returns the
 * total inner tuple count.
 *
 * Slots are created once and reused across M-blocks (s_cap only ever grows),
 * so re-materialising S for the next M-block overwrites existing slots rather
 * than leaking them.  innerDesc is the inner plan's result tuple descriptor.
 *
 * Allocation happens in whatever context is current when ExecRoslNestLoop runs
 * -- the executor's per-query context, not the per-tuple expression context
 * (that one is only entered transiently inside ExecQual/ExecProject and is what
 * ResetExprContext clears).  So s_slots persists across calls, exactly as the
 * m_slots array allocated in rosl_state_init does.
 */
static int
materialise_inner(RoslJoinState *st, PlanState *innerPlan, TupleDesc innerDesc)
{
	int			n = 0;

	for (;;)
	{
		TupleTableSlot *slot = ExecProcNode(innerPlan);

		if (TupIsNull(slot))
			break;

		/* grow the slot array geometrically when we run out of capacity */
		if (n == st->s_cap)
		{
			int			new_cap = (st->s_cap == 0) ? 1024 : st->s_cap * 2;
			int			i;

			/* repalloc() rejects a NULL pointer, so palloc the first block */
			if (st->s_slots == NULL)
				st->s_slots = (TupleTableSlot **)
					palloc(sizeof(TupleTableSlot *) * new_cap);
			else
				st->s_slots = (TupleTableSlot **)
					repalloc(st->s_slots, sizeof(TupleTableSlot *) * new_cap);

			for (i = st->s_cap; i < new_cap; i++)
				st->s_slots[i] = MakeSingleTupleTableSlot(innerDesc);
			st->s_cap = new_cap;
		}

		ExecCopySlot(st->s_slots[n], slot);
		n++;
	}
	return n;
}

/*
 * Epsilon-greedy smoothing over the current M-block, using the FROZEN average
 * joinability from the exploration phase and the fixed epsilon hyperparameter.
 *
 * Because exploration probes every tuple exactly n_probes times, a(t) = n_probes
 * is a known positive constant for all t, so
 *
 *   q(t) = r(t) / n_probes        (always defined; no a(t)=0 fallback branch)
 *
 * Let Qtotal = sum over t in A of q(t).  The single-draw distribution is
 *
 *   p(t) = (1-eps) * q(t)/Qtotal + eps/|A|   if Qtotal > 0
 *        = 1/|A|                             if Qtotal = 0
 *
 * The Qtotal = 0 case (no tuple joined during exploration) falls back to a
 * uniform draw, since there is no joinability signal to weight by.  The result
 * is normalised to a proper distribution.  q(t) and epsilon do not change
 * between K-blocks, so this distribution is identical for every exploitation
 * round within one M-block.
 */
static void
build_distribution(RoslJoinState *st)
{
	int			n = st->m_count;
	int			t;
	double		eps = st->epsilon;
	double		Mn = (double) n;
	double		np = (double) st->n_probes;
	double		Qtot = 0.0;
	double		sum = 0.0;

	/* per-tuple average joinability q(t) = r(t)/n_probes */
	for (t = 0; t < n; t++)
	{
		double		q = (double) st->reward[t] / np;

		st->p[t] = q;				/* stash q(t) here; rescaled below */
		Qtot += q;
	}

	for (t = 0; t < n; t++)
	{
		double		pt;

		if (Qtot > 0.0)
			pt = (1.0 - eps) * (st->p[t] / Qtot) + eps / Mn;
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
		double		r = rosl_rand_double(st);		/* uniform [0,1) */

		st->interim[s] = lower_bound_cum(st->cum, n, r);
	}

	/* sort by M-index, then dedup in a single linear pass */
	qsort(st->interim, L, sizeof(int), cmp_int);

	st->cache_count = 0;
	for (s = 0; s < L; s++)
	{
		if (s == 0 || st->interim[s] != st->interim[s - 1])
			st->cache_midx[st->cache_count++] = st->interim[s];
	}
}

/*
 * Finalise a completed exploitation round (one M-block x one BODY K-block):
 * add this round's Horvitz-Thompson success estimate to the running BODY totals
 * and record the current two-region estimate.
 *
 * Y_hat_round = sum over cached t of  round_match(t) / pi(t)
 * pi(t)       = 1 - (1 - p(t))^L
 * est_den    += Mn * Kn                     (BODY pairs only)
 * mu_hat_B    = est_num / est_den           (mean successes per BODY pair)
 * J_hat       = expl_exact + mu_hat_B * |R| * |B|     (|B| = s_count - pfx)
 *
 * CORRECTION (3): the body extrapolation uses the ACTUAL inner size s_count
 * (the relation is fully materialised) rather than the planner's num_inner, so
 * |B| = s_count - pfx is non-negative by construction -- the old plan|S| -
 * n_probes could go negative and was clamped to 0, silently dropping the whole
 * body term.  pfx = min(n_probes, s_count) is the true prefix width.
 *
 * For |R| the running trajectory uses num_outer (the planner estimate), since
 * the TOTAL outer cardinality is not known until the scan finishes; the final
 * ROSL_SUMM line re-states the estimate with the exact count act_outer.
 *
 * expl_exact is the EXACT match count from region R x P (the shared prefix),
 * accumulated during exploration; it is added in directly (probability-1
 * observations, never IPW-weighted).  The HT mean is extrapolated only over the
 * body |B|, never over the full |S|.
 */
static void
finalize_round(RoslJoinState *st)
{
	double		Yhat = 0.0;
	int			c;

	for (c = 0; c < st->cache_count; c++)
	{
		int			m = st->cache_midx[c];
		double		pm = st->p[m];
		double		pi = 1.0 - pow(1.0 - pm, (double) st->exp_cache_lim);

		if (pi > 0.0)
			Yhat += (double) st->round_match[m] / pi;
	}

	st->est_num += Yhat;
	st->est_den += (double) st->m_count * (double) st->k_count;
	st->rounds++;

	if (st->est_den > 0.0)
	{
		int			pfx = (st->n_probes < st->s_count)
							? st->n_probes : st->s_count;
		double		mu_B = st->est_num / st->est_den;
		double		body_inner = (double) st->s_count - (double) pfx;	/* >= 0 */
		double		Jhat;

		/*
		 * Trajectory |R| proxy = planner num_outer (total outer unknown until
		 * the scan ends).  |B| = s_count - pfx uses the exact inner size.
		 */
		Jhat = st->expl_exact + mu_B * st->num_outer * body_inner;

		/*
		 * Record this round into the in-C trajectory buffer instead of
		 * emitting a mid-stream NOTICE.  Nothing crosses the wire here: the
		 * client drains rows only, and the whole trajectory is dumped once at
		 * teardown by PrintRoslCounters().  This is the Saketh communication
		 * model -- measurement delivery is fully decoupled from row delivery,
		 * so a server-side cursor can never deadlock on notice flushing.
		 *
		 * Timing is taken here, at the source, via GetCurrentTimestamp()
		 * relative to t_start (set when the first M-block loaded).
		 */
		if (st->traj_count < ROSL_TRAJ_CAP)
		{
			double		elapsed_ms = 0.0;

			if (st->t_started)
			{
				/* TimestampTz is int64 microseconds; subtract directly */
				TimestampTz now = GetCurrentTimestamp();

				elapsed_ms = (double) (now - st->t_start) / 1000.0;
			}

			st->traj_round[st->traj_count]          = st->rounds;
			st->traj_mean_per_pair[st->traj_count]  = mu_B;
			st->traj_est_join[st->traj_count]       = Jhat;
			st->traj_pairs_seen[st->traj_count]     = st->est_den;
			st->traj_sample_matches[st->traj_count] = st->sample_matches;
			st->traj_elapsed_ms[st->traj_count]     = elapsed_ms;
			st->traj_count++;
		}
		else
			st->traj_truncated = true;
	}
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
	int			i;

	st = (RoslJoinState *) palloc0(sizeof(RoslJoinState));

	st->m_lim = ROSL_M_LIM;
	st->k_lim = ROSL_K_LIM;
	st->exp_cache_lim = ROSL_EXP_CACHE_LIM;
	st->n_probes = ROSL_N_PROBES;

	/* known pair count for the extrapolation (planner row estimates) */
	st->num_outer = outerPlan(nl)->plan_rows;
	st->num_inner = innerPlan(nl)->plan_rows;

	outerDesc = ExecGetResultType(outerPlanState(node));

	st->m_slots = (TupleTableSlot **) palloc(sizeof(TupleTableSlot *) * st->m_lim);
	for (i = 0; i < st->m_lim; i++)
		st->m_slots[i] = MakeSingleTupleTableSlot(outerDesc);

	/*
	 * S is materialised in full ONCE (lazily, on the first M-block) and grown
	 * on demand by materialise_inner(); start empty so a tiny inner relation
	 * costs almost nothing.  s_built guards against re-materialising per block.
	 */
	st->s_slots = NULL;
	st->s_count = 0;
	st->s_cap = 0;
	st->s_built = false;

	st->reward = (int *) palloc(sizeof(int) * st->m_lim);
	st->round_match = (int *) palloc(sizeof(int) * st->m_lim);
	st->p = (double *) palloc(sizeof(double) * st->m_lim);
	st->cum = (double *) palloc(sizeof(double) * st->m_lim);
	st->interim = (int *) palloc(sizeof(int) * st->exp_cache_lim);
	st->cache_midx = (int *) palloc(sizeof(int) * st->exp_cache_lim);

	st->expl_exact = 0.0;			/* exact R x P match count (region 1)     */
	st->act_outer = 0.0;			/* actual |R|: accumulated per M-block     */

	/* per-round trajectory buffers (dumped once at teardown) */
	st->traj_round          = (long *)   palloc(sizeof(long) * ROSL_TRAJ_CAP);
	st->traj_mean_per_pair  = (double *) palloc(sizeof(double) * ROSL_TRAJ_CAP);
	st->traj_est_join       = (double *) palloc(sizeof(double) * ROSL_TRAJ_CAP);
	st->traj_pairs_seen     = (double *) palloc(sizeof(double) * ROSL_TRAJ_CAP);
	st->traj_sample_matches = (long *)   palloc(sizeof(long) * ROSL_TRAJ_CAP);
	st->traj_elapsed_ms     = (double *) palloc(sizeof(double) * ROSL_TRAJ_CAP);
	st->traj_count          = 0;
	st->traj_truncated      = false;

	st->t_started = false;

	st->epsilon = ROSL_EPSILON;			/* fixed hyperparameter; never decayed */
	st->phase = PH_NEW_MBLOCK;

	/*
	 * Seed the per-state PRNG from a mix of high-resolution time, the backend
	 * PID, and the state pointer, so concurrently launched workers get
	 * independent streams.  (The old srand(time(NULL)) gave same-second workers
	 * identical caches.)  Force non-zero: xorshift64* cannot leave state 0.
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
 *		ExecRoslNestLoop  --  streaming two-phase ROSL sampling join
 *
 *		Consumes R in M-blocks.  For each M-block:
 *		  PH_NEW_MBLOCK   loads the outer block, materialises all of S (once),
 *		                  zeroes r(t), and accumulates the exact outer count.
 *		  PH_EXPLORE      probes every M-tuple against the shared prefix P,
 *		                  counting r(t), tallying matches EXACTLY into expl_exact
 *		                  and emitting them.  q(t) = r(t)/n_probes is frozen at
 *		                  the end of this phase.
 *		  PH_NEW_SBLOCK   takes the next BODY K-block and draws an exploit cache
 *		                  from the epsilon-greedy-on-joinability distribution
 *		                  (built from the frozen q(t) and fixed epsilon).
 *		  PH_PROBE        probes the cache against the K-block, emits matches,
 *		                  and (at round end) folds the body HT estimate.
 *		Resume cursors let every phase yield one row per call.  node->rosl is
 *		guaranteed non-NULL by the dispatcher.
 *
 *		Fixed-Probe semantics: the exploitation distribution is FROZEN for the
 *		whole M-block (q(t) comes only from exploration, which probes every tuple
 *		n_probes times; exploitation never updates r(t)).  epsilon is fixed (a
 *		hyperparameter; never decayed).  Region R x P is counted exactly and the
 *		body HT mean is extrapolated only over |B| = |S| - n_probes, so the
 *		two-region estimate is unbiased with lower variance than pooling.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecRoslNestLoop(NestLoopState *node)
{
	RoslJoinState *st = (RoslJoinState *) node->rosl;
	PlanState  *outerPlan = outerPlanState(node);
	PlanState  *innerPlan = innerPlanState(node);
	TupleDesc	innerDesc = ExecGetResultType(innerPlan);
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
					st->m_count = load_outer_block(st, outerPlan);
					if (st->m_count == 0)
					{
						/*
						 * Outer exhausted: no further M-blocks.  Nothing is
						 * emitted here; matching the Saketh communication model,
						 * the only mid-stream signal the client gets is the
						 * cursor returning NULL.  The full estimate + trajectory
						 * is dumped once at teardown (PrintRoslCounters).
						 */
						st->phase = PH_DONE;
						break;
					}

					/* CORRECTION (3): accumulate EXACT outer cardinality |R| */
					st->act_outer += (double) st->m_count;

					/*
					 * Anchor timing at the first non-empty M-block, i.e. the
					 * moment real work begins.  Measured in C so it is
					 * independent of when the client fetches or libpq flushes.
					 */
					if (!st->t_started)
					{
						st->t_start = GetCurrentTimestamp();
						st->t_started = true;
					}

					/* fresh M-block: zero exploration rewards r(t) */
					memset(st->reward, 0, sizeof(int) * st->m_count);

					/*
					 * Materialise ALL of S ONCE, on the first M-block, and reuse
					 * it for every subsequent M-block.  The relation is shuffled
					 * at rest, so s_slots[0 .. n_probes) is a fixed uniform random
					 * exploration prefix P shared across all M-blocks; the body
					 * s_slots[n_probes .. s_count) is what exploitation scans.
					 * Materialising once (rather than per M-block) is correct
					 * precisely because the prefix and body must be the SAME tuples
					 * for every M-block under global without-replacement.
					 */
					if (!st->s_built)
					{
						ExecReScan(innerPlan);
						st->s_count = materialise_inner(st, innerPlan, innerDesc);
						st->s_built = true;
					}

					if (st->s_count == 0)
					{
						/* empty inner: no joins possible for any M-block */
						st->phase = PH_DONE;
						break;
					}

					/* begin deterministic exploration at (M-tuple 0, probe 0) */
					st->expl_mi = 0;
					st->expl_pi = 0;
					st->phase = PH_EXPLORE;
					break;
				}

			case PH_EXPLORE:
				{
					/*
					 * Deterministic exploration over region R x P: probe every
					 * M-tuple against the SAME shared prefix P = s_slots[0..pfx).
					 * r(t) counts the joins; a(t) is the design constant n_probes
					 * for every tuple, so q(t) = r(t)/n_probes is uncensored.
					 *
					 * These pairs are observed with probability 1, so each match
					 * is (a) emitted as join output and (b) added EXACTLY to
					 * expl_exact -- never inverse-probability weighted.  The
					 * prefix is the same fixed tuples for every M-block, and is
					 * excluded from exploitation, so no pair is probed twice.
					 *
					 * pfx = min(n_probes, s_count): if S is smaller than n_probes
					 * the whole relation is the prefix and there is no body (the
					 * exploitation loop will then find nothing to scan).  Resume
					 * cursors (expl_mi, expl_pi) pick up after each returned row.
					 */
					int			pfx = (st->n_probes < st->s_count)
										? st->n_probes : st->s_count;
					int			mi = st->expl_mi;
					int			pi = st->expl_pi;

					while (mi < st->m_count)
					{
						TupleTableSlot *outerSlot = st->m_slots[mi];

						while (pi < pfx)
						{
							TupleTableSlot *innerSlot = st->s_slots[pi];

							CHECK_FOR_INTERRUPTS();

							econtext->ecxt_outertuple = outerSlot;
							econtext->ecxt_innertuple = innerSlot;
							pi++;			/* advance now; resume picks up here */
							st->t_steps++;

							if (ExecQual(joinqual, econtext))
							{
								if (otherqual == NULL ||
									ExecQual(otherqual, econtext))
								{
									st->reward[mi]++;		/* r(t)               */
									st->expl_exact += 1.0;	/* exact R x P tally   */
									st->sample_matches++;	/* diagnostics         */
									st->expl_mi = mi;		/* save resume pos     */
									st->expl_pi = pi;
									return ExecProject(node->js.ps.ps_ProjInfo);
								}
								else
									InstrCountFiltered2(node, 1);
							}
							else
								InstrCountFiltered1(node, 1);

							ResetExprContext(econtext);
						}
						pi = 0;
						mi++;
					}

					/*
					 * Exploration complete: q(t) is now frozen.  Begin the
					 * exploitation scan at the first BODY K-block, i.e. just
					 * past the shared prefix (k_start = n_probes).  The prefix is
					 * thus excluded from every K-block (global WOR).
					 */
					st->k_start = st->n_probes;
					st->phase = PH_NEW_SBLOCK;
					break;
				}

			case PH_NEW_SBLOCK:
				{
					/* body tuples remaining (k_start began at n_probes) */
					int			remaining = st->s_count - st->k_start;

					if (remaining <= 0)
					{
						/* whole BODY exploited for this M-block: advance outer */
						st->phase = PH_NEW_MBLOCK;
						break;
					}

					st->k_count = (remaining < st->k_lim) ? remaining : st->k_lim;

					/*
					 * Build the epsilon-greedy-on-joinability distribution from
					 * the frozen q(t) and the fixed epsilon.  q(t) and epsilon
					 * are unchanged across K-blocks, so this reproduces the same
					 * distribution each round (matching the pseudocode, which
					 * recomputes it inside the K loop); the cost is O(Mn), small
					 * next to probing.
					 */
					build_distribution(st);		/* p[] from frozen q(t), eps */
					build_cumulative(st);		/* cum[]                     */
					draw_and_dedup_cache(st);	/* -> cache_midx[]           */
					memset(st->round_match, 0, sizeof(int) * st->m_count);

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
					 * Exploitation round: probe the exploit cache against the
					 * current K-block, emitting each matching row immediately.
					 * Per-round match counts feed the Horvitz-Thompson estimator
					 * at round end.  q(t) is FROZEN here -- exploitation does NOT
					 * update r(t) -- so the inclusion probabilities depend only
					 * on the exploration probes, never on what exploitation sees.
					 */
					while (ci < st->cache_count)
					{
						int			m = st->cache_midx[ci];
						TupleTableSlot *outerSlot = st->m_slots[m];

						while (kj < st->k_count)
						{
							TupleTableSlot *innerSlot =
								st->s_slots[st->k_start + kj];

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

					/* Round complete: fold HT estimate, advance to next K-block */
					finalize_round(st);

					st->k_start += st->k_count;	/* next contiguous K-block      */
					st->probe_ci = 0;
					st->probe_kj = 0;
					st->phase = PH_NEW_SBLOCK;	/* epsilon is fixed; no decay   */
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
	 * Plan-shape guard.  The ROSL path materialises the inner ONCE and reuses
	 * it for every M-block, and emits inner-join matches only.  That is correct
	 * only for a plain INNER join with a NON-parameterised inner.  If the
	 * planner picked a parameterised nestloop (nestParams != NIL) or a non-inner
	 * join, running ROSL would silently produce wrong results, so fall back to
	 * the exact stock nested loop even when the GUC is on.  (The experimental
	 * 2-table inner-join queries this estimator targets always satisfy this.)
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
			 "pairs_seen=%.0f sample_matches=%ld elapsed_ms=%.3f",
			 st->traj_round[i],
			 st->traj_mean_per_pair[i],
			 st->traj_est_join[i],
			 st->traj_pairs_seen[i],
			 st->traj_sample_matches[i],
			 st->traj_elapsed_ms[i]);
	}

	if (st->traj_truncated)
		elog(INFO,
			 "ROSL_TRAJ truncated: more than %d rounds; trajectory capped "
			 "(use a smaller scale factor)", ROSL_TRAJ_CAP);

	/*
	 * Final two-region estimate, computed with ACTUAL table sizes (correction
	 * 3).  Region R x P is known exactly (expl_exact); only the body R x B is
	 * estimated, by the HT body mean mu_B = est_num / est_den extrapolated over
	 * |R| * |B|.  Here |R| = act_outer (the exact outer count summed over every
	 * M-block) and |B| = s_count - pfx (the exact materialised inner minus the
	 * exact prefix width), so this final number is exactly unbiased given the
	 * true sizes -- it does NOT depend on the planner's plan_rows.
	 *
	 *   J_hat = expl_exact + mu_B * act_outer * (s_count - pfx)
	 *
	 * The planner estimates num_outer / num_inner are reported alongside for
	 * comparison (and are what the running trajectory used for |R|).  The
	 * `final_est_join=` token is kept verbatim so the existing worker log parser
	 * still finds it.
	 */
	{
		int			pfx = (st->n_probes < st->s_count)
							? st->n_probes : st->s_count;
		double		body_inner = (double) st->s_count - (double) pfx;	/* >= 0 */
		double		mu_body = 0.0;
		double		est_join;

		if (st->est_den > 0.0)
			mu_body = st->est_num / st->est_den;

		est_join = st->expl_exact + mu_body * st->act_outer * body_inner;

		elog(INFO,
			 "ROSL_SUMM final_est_join=%.2f expl_exact=%.0f mu_body=%.10f "
			 "rounds=%ld sample_matches=%ld body_pairs_seen=%.0f t_steps=%ld "
			 "act_outer=%.0f s_count=%d num_outer=%.0f num_inner=%.0f "
			 "n_probes=%d exp_cache_lim=%d epsilon=%.4f",
			 est_join, st->expl_exact, mu_body, st->rounds, st->sample_matches,
			 st->est_den, st->t_steps, st->act_outer, st->s_count,
			 st->num_outer, st->num_inner,
			 st->n_probes, st->exp_cache_lim, st->epsilon);
	}
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
		for (i = 0; i < st->s_cap; i++)
			if (!TupIsNull(st->s_slots[i]))
				ExecDropSingleTupleTableSlot(st->s_slots[i]);

		pfree(st->m_slots);
		if (st->s_slots != NULL)
			pfree(st->s_slots);
		pfree(st->reward);
		pfree(st->round_match);
		pfree(st->p);
		pfree(st->cum);
		pfree(st->interim);
		pfree(st->cache_midx);
		pfree(st->traj_round);
		pfree(st->traj_mean_per_pair);
		pfree(st->traj_est_join);
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
		st->s_count = 0;
		st->s_built = false;			/* re-materialise S (and prefix) fresh */
		st->k_start = 0;
		st->k_count = 0;
		st->cache_count = 0;
		st->expl_mi = 0;
		st->expl_pi = 0;
		st->probe_ci = 0;
		st->probe_kj = 0;
		st->epsilon = ROSL_EPSILON;		/* fixed hyperparameter */
		st->est_num = 0.0;
		st->est_den = 0.0;
		st->expl_exact = 0.0;			/* exact R x P tally restarts          */
		st->act_outer = 0.0;			/* exact |R| accumulator restarts      */
		st->rounds = 0;
		st->t_steps = 0;
		st->sample_matches = 0;
		/* r(t) is zeroed per M-block in PH_NEW_MBLOCK; nothing to do here */

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