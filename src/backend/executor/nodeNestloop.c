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
 * The ROSL path is single-phase.  It consumes R in M-blocks and, for each
 * M-block, scans all of S in K-blocks.  Each (M-block, K-block) round draws
 * a reward-weighted, epsilon-smoothed exploit cache of outer tuples and probes
 * it against the K-block.  Matching pairs are emitted immediately (one per
 * call via resume cursors probe_ci / probe_kj); rewards and per-round match
 * counts are accumulated simultaneously so the Horvitz-Thompson estimator is
 * updated at the end of each round.  Only sampled rows are emitted; this is
 * an approximate join intended for cardinality estimation workloads.
 *
 * INTEGRATION (unchanged):
 * - execnodes.h: add `void *rosl;` to struct NestLoopState.
 * - guc.c: register the bool GUC `enable_rosl` (see notes at end of file).
 * - nodeNestloop.h: unchanged.
 *
 * ROSL ALGORITHM NOTES:
 * R (outer) is consumed in M-blocks of ROSL_M_LIM tuples; for each M-block
 * all of S (inner) is scanned in K-blocks of ROSL_K_LIM tuples.  Each
 * (M-block, K-block) round draws an exploit cache of unique outer tuples
 * from a reward-weighted, epsilon-smoothed distribution and probes it
 * against the K-block, emitting matching rows directly.  Cache fill is one
 * cumulative-probability pass plus a binary search per draw; deduplication
 * is a sort of the drawn indices and a single linear pass.  probe_ci and
 * probe_kj are resume cursors so PH_PROBE can yield one row per call.
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
#define ROSL_EPSILON_FLOOR  0.5    /* Minimum exploration threshold         */

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
	PH_NEW_SBLOCK,					/* load next inner block, draw cache         */
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

	/* outer (R) block buffer */
	TupleTableSlot **m_slots;		/* [m_lim] copies of current M-block      */
	int			m_count;			/* tuples in current M-block (Mn)         */
	int		   *reward;				/* [m_lim] cumulative reward within block */

	/* inner (S) block buffer */
	TupleTableSlot **k_slots;		/* [k_lim] copies of current K-block      */
	int			k_count;			/* tuples in current K-block (Kn)         */

	/* per-round selection distribution and exploit cache */
	double	   *p;					/* [m_lim] single-draw prob (normalised)  */
	double	   *cum;				/* [m_lim] cumulative of p                */
	int		   *interim;			/* [exp_cache_lim] drawn M-indices (dups) */
	int		   *cache_midx;			/* [exp_cache_lim] unique M-indices       */
	int			cache_count;		/* number of unique cached tuples         */
	int		   *round_match;		/* [m_lim] matches this round, by M-index */

	double		epsilon;			/* exploration mass; halved per K-block   */

	/* estimator accumulators (persist across all blocks) */
	double		est_num;			/* sum over rounds of Y_hat_round         */
	double		est_den;			/* sum over rounds of Mn*Kn (pairs seen)  */
	double		num_outer;			/* |R| (planner row estimate)             */
	double		num_inner;			/* |S| (planner row estimate)             */

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
 * Custom epsilon-greedy smoothing over the current M-block, using the rewards
 * accumulated so far in this M-block and the current epsilon:
 *
 * p(t) = (1-eps) * r(t)/R_total + eps/|A|     if r(t) > 0
 * = eps/|A|                              otherwise
 *
 * Normalised to a proper distribution.  Normalisation also gives the correct
 * fallback when no tuple has positive reward yet (every p is eps/|A|, which
 * after normalisation is uniform).
 */
static void
build_distribution(RoslJoinState *st)
{
	int			n = st->m_count;
	int			t;
	double		eps = st->epsilon;
	double		Mn = (double) n;
	double		Rtot = 0.0;
	double		sum = 0.0;

	for (t = 0; t < n; t++)
		if (st->reward[t] > 0)
			Rtot += (double) st->reward[t];

	for (t = 0; t < n; t++)
	{
		double		pt;

		if (st->reward[t] > 0 && Rtot > 0.0)
			pt = (1.0 - eps) * ((double) st->reward[t] / Rtot) + eps / Mn;
		else
			pt = eps / Mn;
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
		double		r = (double) rand() / ((double) RAND_MAX + 1.0);	/* [0,1) */

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
 * Finalise a completed round (one M-block x one K-block): add this round's
 * Horvitz-Thompson success estimate to the running totals and log the current
 * mean-per-pair and extrapolated join-size estimate.
 *
 * Y_hat_round = sum over cached t of  round_match(t) / pi(t)
 * pi(t)       = 1 - (1 - p(t))^L
 * est_den    += Mn * Kn          (FULL-slice pairs)
 * mu_hat      = est_num / est_den
 * J_hat       = mu_hat * |R| * |S|
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
		double		mu = st->est_num / st->est_den;
		double		Jhat = mu * st->num_outer * st->num_inner;

		/*
		 * Record this round into the in-C trajectory buffer instead of
		 * emitting a mid-stream NOTICE.  Nothing crosses the wire here: the
		 * client drains rows only, and the whole trajectory is dumped once at
		 * teardown by PrintRoslCounters().  This is the Saketh communication
		 * model -- measurement delivery is fully decoupled from row delivery,
		 * so a server-side cursor can never deadlock on notice flushing.
		 *
		 * Timing is taken here, at the source, via GetCurrentTimestamp()
		 * relative to t_start (set when the first M-block loaded).  This is
		 * strictly better than the old client-side _TimestampedNotices hack,
		 * which could only observe when libpq happened to flush the NOTICE.
		 */
		if (st->traj_count < ROSL_TRAJ_CAP)
		{
			double		elapsed_ms = 0.0;

			if (st->t_started)
			{
				/*
				 * TimestampTz is an int64 count of microseconds; subtracting
				 * two of them directly avoids depending on the platform's
				 * TimestampDifference() out-param signature (long vs int64),
				 * which has changed across PG versions.
				 */
				TimestampTz now = GetCurrentTimestamp();

				elapsed_ms = (double) (now - st->t_start) / 1000.0;
			}

			st->traj_round[st->traj_count]          = st->rounds;
			st->traj_mean_per_pair[st->traj_count]  = mu;
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
	TupleDesc	innerDesc;
	int			i;

	st = (RoslJoinState *) palloc0(sizeof(RoslJoinState));

	st->m_lim = ROSL_M_LIM;
	st->k_lim = ROSL_K_LIM;
	st->exp_cache_lim = ROSL_EXP_CACHE_LIM;

	/* known pair count for the extrapolation (planner row estimates) */
	st->num_outer = outerPlan(nl)->plan_rows;
	st->num_inner = innerPlan(nl)->plan_rows;

	outerDesc = ExecGetResultType(outerPlanState(node));
	innerDesc = ExecGetResultType(innerPlanState(node));

	st->m_slots = (TupleTableSlot **) palloc(sizeof(TupleTableSlot *) * st->m_lim);
	for (i = 0; i < st->m_lim; i++)
		st->m_slots[i] = MakeSingleTupleTableSlot(outerDesc);
	st->k_slots = (TupleTableSlot **) palloc(sizeof(TupleTableSlot *) * st->k_lim);
	for (i = 0; i < st->k_lim; i++)
		st->k_slots[i] = MakeSingleTupleTableSlot(innerDesc);

	st->reward = (int *) palloc(sizeof(int) * st->m_lim);
	st->round_match = (int *) palloc(sizeof(int) * st->m_lim);
	st->p = (double *) palloc(sizeof(double) * st->m_lim);
	st->cum = (double *) palloc(sizeof(double) * st->m_lim);
	st->interim = (int *) palloc(sizeof(int) * st->exp_cache_lim);
	st->cache_midx = (int *) palloc(sizeof(int) * st->exp_cache_lim);

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

	st->epsilon = 1.0;
	st->phase = PH_NEW_MBLOCK;

	srand((unsigned int) time(NULL));	/* seed the cache-draw RNG once */

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
 *		all of S in K-blocks.  Each round draws a reward-weighted exploit cache
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

					/* fresh M-block: zero rewards, full exploration, rewind S */
					memset(st->reward, 0, sizeof(int) * st->m_count);
					st->epsilon = 1.0;
					ExecReScan(innerPlan);
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

					build_distribution(st);		/* p[] from rewards, eps   */
					build_cumulative(st);		/* cum[]                   */
					draw_and_dedup_cache(st);	/* -> cache_midx[]         */
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

					/* Round complete: fold HT estimate, decay epsilon, next K */
					finalize_round(st);
					
					/* Asymptotic decay: epsilon safely approaches the floor instead of 0 */
					st->epsilon = (st->epsilon + ROSL_EPSILON_FLOOR) / 2.0;
					
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

	CHECK_FOR_INTERRUPTS();

	if (!enable_rosl)
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

	/* final summary */
	if (st->est_den > 0.0)
	{
		double		mu = st->est_num / st->est_den;
		double		est_join = mu * st->num_outer * st->num_inner;

		elog(INFO,
			 "ROSL_SUMM final_est_join=%.2f rounds=%ld sample_matches=%ld "
			 "pairs_seen=%.0f t_steps=%ld num_outer=%.0f num_inner=%.0f",
			 est_join, st->rounds, st->sample_matches, st->est_den,
			 st->t_steps, st->num_outer, st->num_inner);
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

		pfree(st->m_slots);
		pfree(st->k_slots);
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
		st->k_count = 0;
		st->cache_count = 0;
		st->probe_ci = 0;
		st->probe_kj = 0;
		st->epsilon = 1.0;
		st->est_num = 0.0;
		st->est_den = 0.0;
		st->rounds = 0;
		st->t_steps = 0;
		st->sample_matches = 0;

		/* fresh run: discard prior trajectory and re-anchor timing */
		st->traj_count = 0;
		st->traj_truncated = false;
		st->t_started = false;
	}
}