/*-------------------------------------------------------------------------
 *
 * nodeHashjoin.c
 *	  Routines to handle hash join nodes
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHashjoin.c
 *
 * PARALLELISM
 *
 * Hash joins can participate in parallel query execution in several ways.  A
 * parallel-oblivious hash join is one where the node is unaware that it is
 * part of a parallel plan.  In this case, a copy of the inner plan is used to
 * build a copy of the hash table in every backend, and the outer plan could
 * either be built from a partial or complete path, so that the results of the
 * hash join are correspondingly either partial or complete.  A parallel-aware
 * hash join is one that behaves differently, coordinating work between
 * backends, and appears as Parallel Hash Join in EXPLAIN output.  A Parallel
 * Hash Join always appears with a Parallel Hash node.
 *
 * Parallel-aware hash joins use the same per-backend state machine to track
 * progress through the hash join algorithm as parallel-oblivious hash joins.
 * In a parallel-aware hash join, there is also a shared state machine that
 * co-operating backends use to synchronize their local state machines and
 * program counters.  The shared state machine is managed with a Barrier IPC
 * primitive.  When all attached participants arrive at a barrier, the phase
 * advances and all waiting participants are released.
 *
 * When a participant begins working on a parallel hash join, it must first
 * figure out how much progress has already been made, because participants
 * don't wait for each other to begin.  For this reason there are switch
 * statements at key points in the code where we have to synchronize our local
 * state machine with the phase, and then jump to the correct part of the
 * algorithm so that we can get started.
 *
 * One barrier called build_barrier is used to coordinate the hashing phases.
 * The phase is represented by an integer which begins at zero and increments
 * one by one, but in the code it is referred to by symbolic names as follows:
 *
 *   PHJ_BUILD_ELECTING              -- initial state
 *   PHJ_BUILD_ALLOCATING            -- one sets up the batches and table 0
 *   PHJ_BUILD_HASHING_INNER         -- all hash the inner rel
 *   PHJ_BUILD_HASHING_OUTER         -- (multi-batch only) all hash the outer
 *   PHJ_BUILD_DONE                  -- building done, probing can begin
 *
 * While in the phase PHJ_BUILD_HASHING_INNER a separate pair of barriers may
 * be used repeatedly as required to coordinate expansions in the number of
 * batches or buckets.  Their phases are as follows:
 *
 *   PHJ_GROW_BATCHES_ELECTING       -- initial state
 *   PHJ_GROW_BATCHES_ALLOCATING     -- one allocates new batches
 *   PHJ_GROW_BATCHES_REPARTITIONING -- all repartition
 *   PHJ_GROW_BATCHES_FINISHING      -- one cleans up, detects skew
 *
 *   PHJ_GROW_BUCKETS_ELECTING       -- initial state
 *   PHJ_GROW_BUCKETS_ALLOCATING     -- one allocates new buckets
 *   PHJ_GROW_BUCKETS_REINSERTING    -- all insert tuples
 *
 * If the planner got the number of batches and buckets right, those won't be
 * necessary, but on the other hand we might finish up needing to expand the
 * buckets or batches multiple times while hashing the inner relation to stay
 * within our memory budget and load factor target.  For that reason it's a
 * separate pair of barriers using circular phases.
 *
 * The PHJ_BUILD_HASHING_OUTER phase is required only for multi-batch joins,
 * because we need to divide the outer relation into batches up front in order
 * to be able to process batches entirely independently.  In contrast, the
 * parallel-oblivious algorithm simply throws tuples 'forward' to 'later'
 * batches whenever it encounters them while scanning and probing, which it
 * can do because it processes batches in serial order.
 *
 * Once PHJ_BUILD_DONE is reached, backends then split up and process
 * different batches, or gang up and work together on probing batches if there
 * aren't enough to go around.  For each batch there is a separate barrier
 * with the following phases:
 *
 *  PHJ_BATCH_ELECTING       -- initial state
 *  PHJ_BATCH_ALLOCATING     -- one allocates buckets
 *  PHJ_BATCH_LOADING        -- all load the hash table from disk
 *  PHJ_BATCH_PROBING        -- all probe
 *  PHJ_BATCH_DONE           -- end
 *
 * Batch 0 is a special case, because it starts out in phase
 * PHJ_BATCH_PROBING; populating batch 0's hash table is done during
 * PHJ_BUILD_HASHING_INNER so we can skip loading.
 *
 * Initially we try to plan for a single-batch hash join using the combined
 * work_mem of all participants to create a large shared hash table.  If that
 * turns out either at planning or execution time to be impossible then we
 * fall back to regular work_mem sized hash tables.
 *
 * To avoid deadlocks, we never wait for any barrier unless it is known that
 * all other backends attached to it are actively executing the node or have
 * already arrived.  Practically, that means that we never return a tuple
 * while attached to a barrier, unless the barrier has reached its final
 * state.  In the slightly special case of the per-batch barrier, we return
 * tuples while in PHJ_BATCH_PROBING phase, but that's OK because we use
 * BarrierArriveAndDetach() to advance it to PHJ_BATCH_DONE without waiting.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/parallel.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/sharedtuplestore.h"


/*
 * States of the ExecHashJoin state machine
 */
#define HJ_BUILD_HASHTABLE		1
#define HJ_NEED_NEW_OUTER		2
#define HJ_SCAN_BUCKET			3
#define HJ_FILL_OUTER_TUPLE		4
#define HJ_FILL_INNER_TUPLES	5
#define HJ_NEED_NEW_BATCH		6

/*
 * Early Hash Join (EHJ) Phase 1 states.
 *
 * HJ_EHJ_SYMMETRIC
 *		The symmetric in-memory phase.  We alternate reading one tuple from
 *		the inner (R) source and one from the outer (S) source, inserting
 *		each into its half of the double-wide hash table and immediately
 *		probing the opposite half for matches.  This state loops until either
 *		(a) both sources are exhausted, or (b) the memory budget is full
 *		(hashtable->ehj_phase1_done becomes true).
 *
 * HJ_EHJ_SCAN_OUTER_BUCKET
 *		After an inner tuple has been inserted we scan the matching outer
 *		bucket for join partners.  Once exhausted we return to HJ_EHJ_SYMMETRIC
 *		to consume the next pair of tuples.
 *
 * HJ_EHJ_SCAN_INNER_BUCKET
 *		After an outer tuple has been inserted we scan the matching inner
 *		bucket.  Symmetric to HJ_EHJ_SCAN_OUTER_BUCKET.
 */
/*
 * Early Hash Join — Phase 2 states (biased 5:1 reading + spill)
 *
 * HJ_EHJ_PHASE2_LOOP
 *		Driver loop.  Each iteration: invoke biased flush if over budget,
 *		check termination, then read one inner (R) tuple if the inner
 *		read-counter allows it, or one outer (S) tuple otherwise.  When a
 *		tuple is successfully inserted into a non-frozen partition the
 *		state machine transitions to the appropriate probe state.
 *
 * HJ_EHJ_PHASE2_SCAN_OUTER
 *		An inner (R) tuple was just inserted; scan outer_buckets[CurBucketNo]
 *		for matches.  On exhaustion, transition back to HJ_EHJ_PHASE2_LOOP.
 *
 * HJ_EHJ_PHASE2_SCAN_INNER
 *		An outer (S) tuple was just inserted; scan buckets[CurBucketNo]
 *		(the inner half) for matches.  Mirror of PHASE2_SCAN_OUTER.
 *
 * Early Hash Join — Phase 3 states (cleanup join)
 *
 * HJ_EHJ_PHASE3_NEXT_PART
 *		Advance ehj_p3_partno until a partition with at least one non-NULL
 *		disk file is found.  Load all inner and outer tuples for that
 *		partition from their BufFiles into in-memory arrays, reset the
 *		nested-loop cursors, and transition to HJ_EHJ_PHASE3_PROBE.
 *		Returns NULL when all partitions have been processed.
 *
 * HJ_EHJ_PHASE3_PROBE
 *		Nested-loop join over the arrays loaded by PHASE3_NEXT_PART.
 *		For each (inner[ri], outer[si]) pair, applies the EHJ duplicate-
 *		detection timestamp check and the hash join clauses.  Returns one
 *		projected tuple per match.  On exhaustion, pfrees the arrays and
 *		transitions back to HJ_EHJ_PHASE3_NEXT_PART.
 */
#define HJ_EHJ_SYMMETRIC		7
#define HJ_EHJ_SCAN_OUTER_BUCKET 8
#define HJ_EHJ_SCAN_INNER_BUCKET 9
#define HJ_EHJ_PHASE2_LOOP			10
#define HJ_EHJ_PHASE2_SCAN_OUTER	11
#define HJ_EHJ_PHASE2_SCAN_INNER	12
#define HJ_EHJ_PHASE3_NEXT_PART		13
#define HJ_EHJ_PHASE3_PROBE			14

/* Returns true if doing null-fill on outer relation */
#define HJ_FILL_OUTER(hjstate)	((hjstate)->hj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate)	((hjstate)->hj_NullOuterTupleSlot != NULL)

static TupleTableSlot *ExecHashJoinOuterGetTuple(PlanState *outerNode,
						  HashJoinState *hjstate,
						  uint32 *hashvalue);
static TupleTableSlot *ExecParallelHashJoinOuterGetTuple(PlanState *outerNode,
								  HashJoinState *hjstate,
								  uint32 *hashvalue);
static TupleTableSlot *ExecHashJoinGetSavedTuple(HashJoinState *hjstate,
						  BufFile *file,
						  uint32 *hashvalue,
						  TupleTableSlot *tupleSlot);
static bool ExecHashJoinNewBatch(HashJoinState *hjstate);
static bool ExecParallelHashJoinNewBatch(HashJoinState *hjstate);
static void ExecParallelHashJoinPartitionOuter(HashJoinState *node);


/*
 * ExecEHJReadNextTuple
 *		Read one tuple record from an EHJ BufFile.
 *
 * The on-disk format written by ExecEHJFlushPartition() and
 * ExecEHJFlushPartitionBuffer() is:
 *
 *   uint32  hashvalue
 *   int64   arrival_ts
 *   uint32  tuple_len          (= MinimalTuple.t_len)
 *   char[]  tuple_data[tuple_len]
 *
 * Returns true and populates the output pointers on success.
 * Returns false at end-of-file.
 * Raises ereport(ERROR) on any other read failure.
 *
 * The caller is responsible for pfree'ing *tuple_out when done.
 */
static bool
ExecEHJReadNextTuple(BufFile *file,
					 uint32 *hashvalue_out,
					 int64 *ts_out,
					 MinimalTuple *tuple_out)
{
	uint32		hv;
	int64		ts;
	uint32		tlen;
	MinimalTuple tuple;
	size_t		nread;
 
	/* Read hash value — EOF here is the normal end-of-file case. */
	nread = BufFileRead(file, &hv, sizeof(uint32));
	if (nread == 0)
		return false;
	if (nread != sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read hashvalue from EHJ partition file: %m")));
 
	nread = BufFileRead(file, &ts, sizeof(int64));
	if (nread != sizeof(int64))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read arrival_ts from EHJ partition file: %m")));
 
	nread = BufFileRead(file, &tlen, sizeof(uint32));
	if (nread != sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read tuple_len from EHJ partition file: %m")));
 
	tuple = (MinimalTuple) palloc(tlen);
	tuple->t_len = tlen;
 
	/*
	 * MinimalTuple's first field (t_len) is already filled in.  Read the
	 * rest of the tuple body (tlen - sizeof(uint32) bytes) immediately
	 * after it.
	 */
	nread = BufFileRead(file,
						(char *) tuple + sizeof(uint32),
						tlen - sizeof(uint32));
	if (nread != tlen - sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read tuple body from EHJ partition file: %m")));
 
	*hashvalue_out = hv;
	*ts_out = ts;
	*tuple_out = tuple;
	return true;
}
 
/*
 * EHJShouldEmit
 *		Duplicate-detection timestamp check for the EHJ Phase 3 cleanup join.
 *
 * Parameters
 *   tr_ts    arrival timestamp of the inner (R) tuple
 *   ts_ts    arrival timestamp of the outer (S) tuple
 *   tsf_r    flush timestamp of the inner partition  (0 if not flushed)
 *   tsf_s    flush timestamp of the outer partition  (0 if not flushed)
 *   r_flushed / s_flushed — whether the respective partition was frozen
 *
 * Returns true if the pair (R tuple, S tuple) was NOT already emitted
 * during Phases 1 or 2 and therefore must be emitted now.
 *
 * The three cases are taken directly from Section 4.3 of the EHJ paper
 * (Lawrence 2005), translated from Python's VanillaEHJ.should_emit():
 *
 *   Case 1: S tuple arrived before S partition flushed, R tuple arrived
 *           after S partition flushed.
 *             → ts_ts <= tsf_s  AND  tr_ts > tsf_s
 *
 *   Case 2: S tuple arrived after S partition flushed but before R partition
 *           flushed, and R tuple arrived after S tuple.
 *             → ts_ts > tsf_s  AND  ts_ts <= tsf_r  AND  tr_ts > ts_ts
 *
 *   Case 3: S tuple arrived after both partitions were flushed.
 *             → ts_ts > tsf_r
 *
 * If neither partition was flushed the pair was handled in Phase 1; return
 * false.  If only S was flushed, the special-case from the Python code
 * (tsf_r is None) simplifies to: emit iff ts_ts > tsf_s, which is Case 3
 * with tsf_r treated as infinity — handled by the (!r_flushed) branch.
 */
static bool
EHJShouldEmit(int64 tr_ts, int64 ts_ts,
			  int64 tsf_r, int64 tsf_s,
			  bool r_flushed, bool s_flushed)
{
	if (!s_flushed && !r_flushed)
		return false;
 
	if (!s_flushed)
	{
		/*
		 * Only R partition was flushed.  The Python analogue: tsf_s is None,
		 * return ts_ts > tsf_r.
		 */
		return ts_ts > tsf_r;
	}
 
	/* Case 1 */
	if (ts_ts <= tsf_s && tr_ts > tsf_s)
		return true;
 
	if (r_flushed)
	{
		/* Case 2 */
		if (ts_ts > tsf_s && ts_ts <= tsf_r && tr_ts > ts_ts)
			return true;
		/* Case 3 */
		if (ts_ts > tsf_r)
			return true;
	}
 
	return false;
}

/* ----------------------------------------------------------------
 *		ExecHashJoinImpl
 *
 *		This function implements the Hybrid Hashjoin algorithm.  It is marked
 *		with an always-inline attribute so that ExecHashJoin() and
 *		ExecParallelHashJoin() can inline it.  Compilers that respect the
 *		attribute should create versions specialized for parallel == true and
 *		parallel == false with unnecessary branches removed.
 *
 *		Note: the relation we build hash table on is the "inner"
 *			  the other one is "outer".
 * ----------------------------------------------------------------
 */
static pg_attribute_always_inline TupleTableSlot *
ExecHashJoinImpl(PlanState *pstate, bool parallel)
{
	HashJoinState *node = castNode(HashJoinState, pstate);
	PlanState  *outerNode;
	HashState  *hashNode;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	HashJoinTable hashtable;
	TupleTableSlot *outerTupleSlot;
	uint32		hashvalue;
	int			batchno;
	ParallelHashJoinState *parallel_state;

	/*
	 * get information from HashJoin node
	 */
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	hashNode = (HashState *) innerPlanState(node);
	outerNode = outerPlanState(node);
	hashtable = node->hj_HashTable;
	econtext = node->js.ps.ps_ExprContext;
	parallel_state = hashNode->parallel_state;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * run the hash join state machine
	 */
	for (;;)
	{
		/*
		 * It's possible to iterate this loop many times before returning a
		 * tuple, in some pathological cases such as needing to move much of
		 * the current batch to a later batch.  So let's check for interrupts
		 * each time through.
		 */
		CHECK_FOR_INTERRUPTS();

		switch (node->hj_JoinState)
		{
			case HJ_BUILD_HASHTABLE:

				/*
				 * First time through: build hash table for inner relation.
				 */
				Assert(hashtable == NULL);

				/*
				 * If the outer relation is completely empty, and it's not
				 * right/full join, we can quit without building the hash
				 * table.  However, for an inner join it is only a win to
				 * check this when the outer relation's startup cost is less
				 * than the projected cost of building the hash table.
				 * Otherwise it's best to build the hash table first and see
				 * if the inner relation is empty.  (When it's a left join, we
				 * should always make this check, since we aren't going to be
				 * able to skip the join on the strength of an empty inner
				 * relation anyway.)
				 *
				 * If we are rescanning the join, we make use of information
				 * gained on the previous scan: don't bother to try the
				 * prefetch if the previous scan found the outer relation
				 * nonempty. This is not 100% reliable since with new
				 * parameters the outer relation might yield different
				 * results, but it's a good heuristic.
				 *
				 * The only way to make the check is to try to fetch a tuple
				 * from the outer plan node.  If we succeed, we have to stash
				 * it away for later consumption by ExecHashJoinOuterGetTuple.
				 */
				if (HJ_FILL_INNER(node))
				{
					/* no chance to not build the hash table */
					node->hj_FirstOuterTupleSlot = NULL;
				}
				else if (parallel)
				{
					/*
					 * The empty-outer optimization is not implemented for
					 * shared hash tables, because no one participant can
					 * determine that there are no outer tuples, and it's not
					 * yet clear that it's worth the synchronization overhead
					 * of reaching consensus to figure that out.  So we have
					 * to build the hash table.
					 */
					node->hj_FirstOuterTupleSlot = NULL;
				}
				else if (HJ_FILL_OUTER(node) ||
						 (outerNode->plan->startup_cost < hashNode->ps.plan->total_cost &&
						  !node->hj_OuterNotEmpty))
				{
					node->hj_FirstOuterTupleSlot = ExecProcNode(outerNode);
					if (TupIsNull(node->hj_FirstOuterTupleSlot))
					{
						node->hj_OuterNotEmpty = false;
						return NULL;
					}
					else
						node->hj_OuterNotEmpty = true;
				}
				else
					node->hj_FirstOuterTupleSlot = NULL;

				/*
				 * Create the hash table.  If using Parallel Hash, then
				 * whoever gets here first will create the hash table and any
				 * later arrivals will merely attach to it.
				 */
				hashtable = ExecHashTableCreate(hashNode,
												node->hj_HashOperators,
												HJ_FILL_INNER(node));
				node->hj_HashTable = hashtable;

				/*
				 * Execute the Hash node, to build the hash table.  If using
				 * Parallel Hash, then we'll try to help hashing unless we
				 * arrived too late.
				 */
				hashNode->hashtable = hashtable;
				(void) MultiExecProcNode((PlanState *) hashNode);

				/*
				 * If the inner relation is completely empty, and we're not
				 * doing a left outer join, we can quit without scanning the
				 * outer relation.
				 */
				if (hashtable->totalTuples == 0 && !HJ_FILL_OUTER(node))
					return NULL;

				/*
				 * need to remember whether nbatch has increased since we
				 * began scanning the outer relation
				 */
				hashtable->nbatch_outstart = hashtable->nbatch;

				/*
				 * Reset OuterNotEmpty for scan.  (It's OK if we fetched a
				 * tuple above, because ExecHashJoinOuterGetTuple will
				 * immediately set it again.)
				 */
				node->hj_OuterNotEmpty = false;

				if (parallel)
				{
					Barrier    *build_barrier;

					build_barrier = &parallel_state->build_barrier;
					Assert(BarrierPhase(build_barrier) == PHJ_BUILD_HASHING_OUTER ||
						   BarrierPhase(build_barrier) == PHJ_BUILD_DONE);
					if (BarrierPhase(build_barrier) == PHJ_BUILD_HASHING_OUTER)
					{
						/*
						 * If multi-batch, we need to hash the outer relation
						 * up front.
						 */
						if (hashtable->nbatch > 1)
							ExecParallelHashJoinPartitionOuter(node);
						BarrierArriveAndWait(build_barrier,
											 WAIT_EVENT_HASH_BUILD_HASHING_OUTER);
					}
					Assert(BarrierPhase(build_barrier) == PHJ_BUILD_DONE);

					/* Each backend should now select a batch to work on. */
					hashtable->curbatch = -1;
					node->hj_JoinState = HJ_NEED_NEW_BATCH;

					continue;
				}
				else
					node->hj_JoinState = HJ_NEED_NEW_OUTER;

				/* FALL THRU */

			case HJ_NEED_NEW_OUTER:

				/*
				 * We don't have an outer tuple, try to get the next one
				 */
				if (parallel)
					outerTupleSlot =
						ExecParallelHashJoinOuterGetTuple(outerNode, node,
														  &hashvalue);
				else
					outerTupleSlot =
						ExecHashJoinOuterGetTuple(outerNode, node, &hashvalue);

				if (TupIsNull(outerTupleSlot))
				{
					/* end of batch, or maybe whole join */
					if (HJ_FILL_INNER(node))
					{
						/* set up to scan for unmatched inner tuples */
						ExecPrepHashTableForUnmatched(node);
						node->hj_JoinState = HJ_FILL_INNER_TUPLES;
					}
					else
						node->hj_JoinState = HJ_NEED_NEW_BATCH;
					continue;
				}

				econtext->ecxt_outertuple = outerTupleSlot;
				node->hj_MatchedOuter = false;

				/*
				 * Find the corresponding bucket for this tuple in the main
				 * hash table or skew hash table.
				 */
				node->hj_CurHashValue = hashvalue;
				ExecHashGetBucketAndBatch(hashtable, hashvalue,
										  &node->hj_CurBucketNo, &batchno);
				node->hj_CurSkewBucketNo = ExecHashGetSkewBucket(hashtable,
																 hashvalue);
				node->hj_CurTuple = NULL;

				/*
				 * The tuple might not belong to the current batch (where
				 * "current batch" includes the skew buckets if any).
				 */
				if (batchno != hashtable->curbatch &&
					node->hj_CurSkewBucketNo == INVALID_SKEW_BUCKET_NO)
				{
					/*
					 * Need to postpone this outer tuple to a later batch.
					 * Save it in the corresponding outer-batch file.
					 */
					Assert(parallel_state == NULL);
					Assert(batchno > hashtable->curbatch);
					ExecHashJoinSaveTuple(ExecFetchSlotMinimalTuple(outerTupleSlot),
										  hashvalue,
										  &hashtable->outerBatchFile[batchno]);

					/* Loop around, staying in HJ_NEED_NEW_OUTER state */
					continue;
				}

				/* OK, let's scan the bucket for matches */
				node->hj_JoinState = HJ_SCAN_BUCKET;

				/* FALL THRU */

			case HJ_SCAN_BUCKET:

				/*
				 * Scan the selected hash bucket for matches to current outer
				 */
				if (parallel)
				{
					if (!ExecParallelScanHashBucket(node, econtext))
					{
						/* out of matches; check for possible outer-join fill */
						node->hj_JoinState = HJ_FILL_OUTER_TUPLE;
						continue;
					}
				}
				else
				{
					if (!ExecScanHashBucket(node, econtext))
					{
						/* out of matches; check for possible outer-join fill */
						node->hj_JoinState = HJ_FILL_OUTER_TUPLE;
						continue;
					}
				}

				/*
				 * We've got a match, but still need to test non-hashed quals.
				 * ExecScanHashBucket already set up all the state needed to
				 * call ExecQual.
				 *
				 * If we pass the qual, then save state for next call and have
				 * ExecProject form the projection, store it in the tuple
				 * table, and return the slot.
				 *
				 * Only the joinquals determine tuple match status, but all
				 * quals must pass to actually return the tuple.
				 */
				if (joinqual == NULL || ExecQual(joinqual, econtext))
				{
					node->hj_MatchedOuter = true;
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple));

					/* In an antijoin, we never return a matched tuple */
					if (node->js.jointype == JOIN_ANTI)
					{
						node->hj_JoinState = HJ_NEED_NEW_OUTER;
						continue;
					}

					/*
					 * If we only need to join to the first matching inner
					 * tuple, then consider returning this one, but after that
					 * continue with next outer tuple.
					 */
					if (node->js.single_match)
						node->hj_JoinState = HJ_NEED_NEW_OUTER;

					if (otherqual == NULL || ExecQual(otherqual, econtext))
						return ExecProject(node->js.ps.ps_ProjInfo);
					else
						InstrCountFiltered2(node, 1);
				}
				else
					InstrCountFiltered1(node, 1);
				break;

			case HJ_FILL_OUTER_TUPLE:

				/*
				 * The current outer tuple has run out of matches, so check
				 * whether to emit a dummy outer-join tuple.  Whether we emit
				 * one or not, the next state is NEED_NEW_OUTER.
				 */
				node->hj_JoinState = HJ_NEED_NEW_OUTER;

				if (!node->hj_MatchedOuter &&
					HJ_FILL_OUTER(node))
				{
					/*
					 * Generate a fake join tuple with nulls for the inner
					 * tuple, and return it if it passes the non-join quals.
					 */
					econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;

					if (otherqual == NULL || ExecQual(otherqual, econtext))
						return ExecProject(node->js.ps.ps_ProjInfo);
					else
						InstrCountFiltered2(node, 1);
				}
				break;

			case HJ_FILL_INNER_TUPLES:

				/*
				 * We have finished a batch, but we are doing right/full join,
				 * so any unmatched inner tuples in the hashtable have to be
				 * emitted before we continue to the next batch.
				 */
				if (!ExecScanHashTableForUnmatched(node, econtext))
				{
					/* no more unmatched tuples */
					node->hj_JoinState = HJ_NEED_NEW_BATCH;
					continue;
				}

				/*
				 * Generate a fake join tuple with nulls for the outer tuple,
				 * and return it if it passes the non-join quals.
				 */
				econtext->ecxt_outertuple = node->hj_NullOuterTupleSlot;

				if (otherqual == NULL || ExecQual(otherqual, econtext))
					return ExecProject(node->js.ps.ps_ProjInfo);
				else
					InstrCountFiltered2(node, 1);
				break;

			case HJ_NEED_NEW_BATCH:

				/*
				 * Try to advance to next batch.  Done if there are no more.
				 */
				if (parallel)
				{
					if (!ExecParallelHashJoinNewBatch(node))
						return NULL;	/* end of parallel-aware join */
				}
				else
				{
					if (!ExecHashJoinNewBatch(node))
						return NULL;	/* end of parallel-oblivious join */
				}
				node->hj_JoinState = HJ_NEED_NEW_OUTER;
				break;

			/* --------------------------------------------------------
			 * Early Hash Join — Phase 1 states
			 * -------------------------------------------------------- */

			case HJ_EHJ_SYMMETRIC:
			{
				/*
				 * Alternate reading one inner (R) and one outer (S) tuple per
				 * iteration.  For each tuple we:
				 *   1. Fetch the MinimalTuple from the slot BEFORE calling
				 *      ExecHashGetHashValue, because ExecHashGetHashValue calls
				 *      ResetExprContext which can free per-tuple memory that
				 *      the slot references.
				 *   2. Hash the tuple.
				 *   3. Insert the MinimalTuple copy into the appropriate bucket.
				 *   4. Set both econtext slots correctly for projection, then
				 *      transition to the matching scan state.
				 *
				 * Plan-tree layout reminder:
				 *   HashJoin.lefttree  → outer (S) scan
				 *   HashJoin.righttree → Hash node
				 *                          Hash.outerPlan → inner (R) scan
				 *
				 * We call ExecProcNode on the scan nodes, not on the Hash node
				 * itself (the Hash node's ExecProcNode is an error stub).
				 */
				HashState	   *hashNode_ehj =
					(HashState *) innerPlanState(node);
				PlanState	   *innerScan_ehj =
					outerPlanState(hashNode_ehj);	/* R tuples */
				PlanState	   *outerScan_ehj =
					outerPlanState(node);			/* S tuples */
				ExprContext    *inner_econtext =
					hashNode_ehj->ps.ps_ExprContext;
				HashJoinTable	hashtable_ehj = node->hj_HashTable;
				TupleTableSlot *slot;
				MinimalTuple	inner_tuple = NULL;
				MinimalTuple	outer_tuple = NULL;
				uint32			inner_hashvalue = 0;
				uint32			outer_hashvalue = 0;
				int				inner_bucketno = 0;
				int				outer_bucketno = 0;
				int				dummy_batchno;
				bool			got_inner = false;
				bool			got_outer = false;

				/* First entry: create the EHJ hash table. */
				if (hashtable_ehj == NULL)
				{
					elog(INFO, "EHJ Status: Starting Phase 1 (Symmetric Ping-Pong) execution.");
					hashtable_ehj = ExecEHJHashTableCreate(
						hashNode_ehj,
						node->hj_HashOperators,
						HJ_FILL_INNER(node));
					node->hj_HashTable = hashtable_ehj;
					hashNode_ehj->hashtable = hashtable_ehj;
				}

				Assert(hashtable_ehj->ehj_enabled);

				/* ---- consume one inner (R) tuple ---- */
				if (!hashtable_ehj->ehj_inner_done &&
					!hashtable_ehj->ehj_phase1_done)
				{
					slot = ExecProcNode(innerScan_ehj);
					if (TupIsNull(slot))
					{
						hashtable_ehj->ehj_inner_done = true;
					}
					else
					{
						/*
						 * Materialize BEFORE hashing: ExecHashGetHashValue
						 * calls ResetExprContext which frees per-tuple memory.
						 * ExecCopySlotMinimalTuple allocates in the current
						 * memory context (per-query), so it survives the reset.
						 */
						inner_tuple = ExecCopySlotMinimalTuple(slot);

						inner_econtext->ecxt_innertuple = slot;
						if (ExecHashGetHashValue(hashtable_ehj,
												  inner_econtext,
												  node->hj_InnerHashKeys,
												  false, /* inner tuple */
												  hashtable_ehj->keepNulls,
												  &inner_hashvalue))
						{
							ExecEHJTableInsertInner(hashtable_ehj,
													inner_tuple,
													inner_hashvalue);
							if (!hashtable_ehj->ehj_phase1_done)
							{
								ExecHashGetBucketAndBatch(hashtable_ehj,
														  inner_hashvalue,
														  &inner_bucketno,
														  &dummy_batchno);
								got_inner = true;
							}
						}
						/* inner_tuple is now safely in the slab; no pfree needed */
					}
				}

				/* ---- consume one outer (S) tuple ---- */
				if (!hashtable_ehj->ehj_outer_done &&
					!hashtable_ehj->ehj_phase1_done)
				{
					slot = ExecProcNode(outerScan_ehj);
					if (TupIsNull(slot))
					{
						hashtable_ehj->ehj_outer_done = true;
					}
					else
					{
						/* Same pre-materialization discipline as inner. */
						outer_tuple = ExecCopySlotMinimalTuple(slot);

						econtext->ecxt_outertuple = slot;
						if (ExecHashGetHashValue(hashtable_ehj,
												  econtext,
												  node->hj_OuterHashKeys,
												  true,  /* outer tuple */
												  HJ_FILL_OUTER(node),
												  &outer_hashvalue))
						{
							ExecEHJTableInsertOuter(hashtable_ehj,
													outer_tuple,
													outer_hashvalue);
							if (!hashtable_ehj->ehj_phase1_done)
							{
								ExecHashGetBucketAndBatch(hashtable_ehj,
														  outer_hashvalue,
														  &outer_bucketno,
														  &dummy_batchno);
								node->hj_OuterNotEmpty = true;
								got_outer = true;
							}
						}
					}
				}

			if (hashtable_ehj->ehj_phase1_done ||
				(hashtable_ehj->ehj_inner_done &&
				 hashtable_ehj->ehj_outer_done))
			{
				if (hashtable_ehj->ehj_inner_done &&
					hashtable_ehj->ehj_outer_done)
				{
					/*
					 * Both sources exhausted without filling memory: skip
					 * Phase 2 and go directly to Phase 3 cleanup.  Drain
					 * any post-freeze buffers (there may be none, but the
					 * call is safe and cheap).
					 */
					ExecEHJFlushAllPartitionBuffers(hashtable_ehj);
					node->ehj_p3_partno = 0;
					node->hj_JoinState = HJ_EHJ_PHASE3_NEXT_PART;
					elog(INFO,
						 "EHJ Status: Phase 1 complete (sources exhausted). "
						 "Entering Phase 3 cleanup.");
				}
				else
				{
					/*
					 * Memory filled before sources were exhausted: enter
					 * Phase 2.  Configure the 5:1 (R:S) reading strategy
					 * described in Section 4.2 of the EHJ paper and reset
					 * the per-cycle counters.
					 */
					hashtable_ehj->ehj_phase1_done = true;
					node->read_ratio_inner = 5;
					node->read_ratio_outer = 1;
					node->reads_from_inner = 0;
					node->reads_from_outer = 0;
					node->ehj_p2_pending_inner = false;
					node->ehj_p2_pending_outer = false;
					node->hj_JoinState = HJ_EHJ_PHASE2_LOOP;
					elog(INFO,
						 "EHJ Status: Phase 1 complete (memory full). "
						 "Entering Phase 2 (biased 5:1 flush/spill).");
				}
				continue;
			}

				/*
				 * Transition to probe states.
				 *
				 * When an inner tuple was inserted, set econtext so that
				 * ExecProject can build the result:
				 *   ecxt_innertuple = the new inner tuple  (hj_HashTupleSlot)
				 *   ecxt_outertuple = matched outer tuple  (set by ScanOuterBucket)
				 *
				 * When only an outer tuple was inserted:
				 *   ecxt_outertuple = the new outer tuple  (hj_OuterTupleSlot)
				 *   ecxt_innertuple = matched inner tuple  (set by ScanInnerBucket)
				 *
				 * If both were inserted we service the inner probe first;
				 * the outer probe is deferred to the next SYMMETRIC iteration
				 * (the outer tuple is already in the table, so it will be
				 * matched by future inner insertions).
				 */
				if (got_inner)
				{
					/*
					 * Store the inner tuple in hj_HashTupleSlot so that
					 * ExecProject sees it as ecxt_innertuple.
					 */
					ExecStoreMinimalTuple(inner_tuple,
										  node->hj_HashTupleSlot,
										  false); /* do not pfree — in slab */
					econtext->ecxt_innertuple = node->hj_HashTupleSlot;

					node->hj_CurBucketNo = inner_bucketno;
					node->hj_CurHashValue = inner_hashvalue;
					node->hj_CurTuple = NULL;
					node->hj_JoinState = HJ_EHJ_SCAN_OUTER_BUCKET;
					continue;
				}
				if (got_outer)
				{
					/*
					 * Store the outer tuple in hj_OuterTupleSlot so that
					 * ExecProject sees it as ecxt_outertuple.
					 */
					ExecStoreMinimalTuple(outer_tuple,
										  node->hj_OuterTupleSlot,
										  false); /* do not pfree — in slab */
					econtext->ecxt_outertuple = node->hj_OuterTupleSlot;

					node->hj_CurBucketNo = outer_bucketno;
					node->hj_CurHashValue = outer_hashvalue;
					node->hj_CurTuple = NULL;
					node->hj_JoinState = HJ_EHJ_SCAN_INNER_BUCKET;
					continue;
				}

				/* Nothing inserted this round (both NULL tuples) — loop */
				break;
			}

			case HJ_EHJ_SCAN_OUTER_BUCKET:
			{
				/*
				 * We just inserted an inner (R) tuple; scan the outer (S)
				 * bucket for matches.
				 *
				 * econtext->ecxt_innertuple was set by the insert loop above.
				 * We scan outer_buckets[hj_CurBucketNo] using hj_CurTuple as
				 * the cursor and hj_CurHashValue as the hash filter.
				 */
				if (!ExecEHJScanOuterBucket(node, econtext,
											node->hj_CurHashValue))
				{
					/*
					 * Outer bucket exhausted.  If we also have a pending outer
					 * probe (got_outer was true when we entered the SYMMETRIC
					 * state and stored the outer bucket in hj_CurSkewBucketNo),
					 * service it now.
					 */
					if (node->hj_CurSkewBucketNo != INVALID_SKEW_BUCKET_NO)
					{
						/*
						 * Restore outer probe context.  The outer hashvalue was
						 * not saved separately — we cannot reconstruct it here
						 * without re-hashing.  For simplicity we skip the
						 * deferred outer probe and clear the flag; the tuple
						 * will be re-encountered during Phase 2 if needed.
						 *
						 * TODO: save the deferred outer hash value in a new
						 * HashJoinState field to enable the deferred probe.
						 */
						node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
					}
					node->hj_CurTuple = NULL;
					node->hj_JoinState = HJ_EHJ_SYMMETRIC;
					break;
				}

				/* Match found — project and return */
				if (node->js.jointype == JOIN_ANTI)
				{
					node->hj_JoinState = HJ_EHJ_SYMMETRIC;
					node->hj_CurTuple = NULL;
					break;
				}
				if (node->js.single_match)
				{
					node->hj_JoinState = HJ_EHJ_SYMMETRIC;
					node->hj_CurTuple = NULL;
				}
				if (otherqual == NULL || ExecQual(otherqual, econtext))
					return ExecProject(node->js.ps.ps_ProjInfo);
				else
					InstrCountFiltered2(node, 1);
				break;
			}

			case HJ_EHJ_SCAN_INNER_BUCKET:
			{
				/*
				 * We just inserted an outer (S) tuple; scan the inner (R)
				 * bucket for matches.
				 *
				 * econtext->ecxt_outertuple was set by the insert loop above.
				 */
				if (!ExecEHJScanInnerBucket(node, econtext,
											node->hj_CurHashValue))
				{
					node->hj_CurTuple = NULL;
					node->hj_JoinState = HJ_EHJ_SYMMETRIC;
					break;
				}

				if (node->js.jointype == JOIN_ANTI)
				{
					node->hj_JoinState = HJ_EHJ_SYMMETRIC;
					node->hj_CurTuple = NULL;
					break;
				}
				if (node->js.single_match)
				{
					node->hj_JoinState = HJ_EHJ_SYMMETRIC;
					node->hj_CurTuple = NULL;
				}
				if (otherqual == NULL || ExecQual(otherqual, econtext))
					return ExecProject(node->js.ps.ps_ProjInfo);
				else
					InstrCountFiltered2(node, 1);
				break;
			}
			case HJ_EHJ_PHASE2_LOOP:
			{
				HashJoinTable ht = node->hj_HashTable;
				HashState	*hn = (HashState *) innerPlanState(node);
				PlanState	*innerScan = outerPlanState(hn);
				PlanState	*outerScan = outerPlanState(node);
				ExprContext *inner_econtext = hn->ps.ps_ExprContext;
				TupleTableSlot *slot;
				MinimalTuple tup;
				uint32		hv;
				int			bucketno,
							dummy_batchno;
	
				/* ---- Biased flush: relieve memory pressure ---- */
				if (ht->spaceUsed >= ht->spaceAllowed)
				{
					if (!ExecEHJBiasedFlush(ht))
						elog(WARNING,
							"EHJ Phase 2: could not relieve memory pressure; "
							"all partitions are frozen.");
				}
	
				/* ---- Termination check ---- */
				if (ht->ehj_inner_done && ht->ehj_outer_done)
				{
					ExecEHJFlushAllPartitionBuffers(ht);
					node->ehj_p3_partno = 0;
					node->hj_JoinState = HJ_EHJ_PHASE3_NEXT_PART;
					elog(INFO,
						"EHJ Status: Phase 2 complete. "
						"Entering Phase 3 cleanup.");
					continue;
				}
	
				/*
				* Process one tuple per state-machine tick.
				*
				* The reading strategy is A:B = read_ratio_inner : read_ratio_outer.
				* We read from the inner (R) source for read_ratio_inner ticks,
				* then switch to the outer (S) source for read_ratio_outer ticks,
				* and so on.  The counters reads_from_inner / reads_from_outer
				* track position within the current half-cycle.
				*/
				if (node->reads_from_inner < node->read_ratio_inner &&
					!ht->ehj_inner_done)
				{
					/* ---- Read one inner (R) tuple ---- */
					slot = ExecProcNode(innerScan);
					node->reads_from_inner++;
	
					if (TupIsNull(slot))
					{
						ht->ehj_inner_done = true;
						/* Reset counter so outer phase can proceed */
						node->reads_from_inner = node->read_ratio_inner;
						break;
					}
	
					tup = ExecCopySlotMinimalTuple(slot);
					inner_econtext->ecxt_innertuple = slot;
	
					if (!ExecHashGetHashValue(ht, inner_econtext,
											node->hj_InnerHashKeys,
											false, ht->keepNulls, &hv))
					{
						pfree(tup);
						break;
					}
	
					ExecHashGetBucketAndBatch(ht, hv, &bucketno, &dummy_batchno);
	
					if (ht->ehj_outer_parts[bucketno].is_flushed)
					{
						/*
						* The corresponding outer partition is frozen.  Per the
						* biased flushing policy, spill this R tuple directly to
						* the inner partition's disk file (it will be needed in
						* Phase 3 when we join the two on-disk partitions).
						*/
						ExecEHJBufferFrozenTuple(ht,
												&ht->ehj_inner_parts[bucketno],
												tup, hv,
												ht->ehj_current_tick + 1);
						ht->ehj_current_tick++;
						pfree(tup);	/* buffered copy was made by BufferFrozenTuple */
					}
					else
					{
						/*
						* Insert into the inner block chain and probe the outer
						* bucket for existing S tuples that match.
						*/
						ExecEHJTableInsertInner(ht, tup, hv);
	
						ExecStoreMinimalTuple(tup, node->hj_HashTupleSlot, false);
						econtext->ecxt_innertuple = node->hj_HashTupleSlot;
	
						node->hj_CurBucketNo = bucketno;
						node->hj_CurHashValue = hv;
						node->hj_CurTuple = NULL;
						node->ehj_p2_pending_inner = false;
						node->hj_JoinState = HJ_EHJ_PHASE2_SCAN_OUTER;
					}
	
					pfree(tup);
					break;
				}
				else if (node->reads_from_inner >= node->read_ratio_inner &&
						node->reads_from_outer < node->read_ratio_outer &&
						!ht->ehj_outer_done)
				{
					/* ---- Read one outer (S) tuple ---- */
					slot = ExecProcNode(outerScan);
					node->reads_from_outer++;
	
					if (TupIsNull(slot))
					{
						ht->ehj_outer_done = true;
						/* Reset both counters to start a new cycle */
						node->reads_from_inner = 0;
						node->reads_from_outer = 0;
						break;
					}
	
					tup = ExecCopySlotMinimalTuple(slot);
					econtext->ecxt_outertuple = slot;
	
					if (!ExecHashGetHashValue(ht, econtext,
											node->hj_OuterHashKeys,
											true, HJ_FILL_OUTER(node), &hv))
					{
						pfree(tup);
						/* Advance cycle */
						node->reads_from_inner = 0;
						node->reads_from_outer = 0;
						break;
					}
	
					ExecHashGetBucketAndBatch(ht, hv, &bucketno, &dummy_batchno);
	
					if (ht->ehj_inner_parts[bucketno].is_flushed)
					{
						/*
						* The inner partition is frozen: spill this S tuple to
						* the outer partition's disk file.
						*/
						ExecEHJBufferFrozenTuple(ht,
												&ht->ehj_outer_parts[bucketno],
												tup, hv,
												ht->ehj_current_tick + 1);
						ht->ehj_current_tick++;
						pfree(tup);
					}
					else
					{
						ExecEHJTableInsertOuter(ht, tup, hv);
	
						ExecStoreMinimalTuple(tup, node->hj_OuterTupleSlot, false);
						econtext->ecxt_outertuple = node->hj_OuterTupleSlot;
	
						node->hj_CurBucketNo = bucketno;
						node->hj_CurHashValue = hv;
						node->hj_CurTuple = NULL;
						node->ehj_p2_pending_outer = false;
						node->hj_JoinState = HJ_EHJ_PHASE2_SCAN_INNER;
					}
	
					pfree(tup);
	
					/* Completed one full A:B cycle; reset counters. */
					if (node->reads_from_outer >= node->read_ratio_outer)
					{
						node->reads_from_inner = 0;
						node->reads_from_outer = 0;
					}
					break;
				}
				else
				{
					/* Reset cycle counters and loop. */
					node->reads_from_inner = 0;
					node->reads_from_outer = 0;
				}
				break;
			}
	
			case HJ_EHJ_PHASE2_SCAN_OUTER:
			{
				/*
				* An inner (R) tuple was just inserted in PHASE2_LOOP.
				* Scan the outer bucket for matches.
				*
				* econtext->ecxt_innertuple = inserted R tuple (hj_HashTupleSlot)
				* Scan sets econtext->ecxt_outertuple on each match.
				*/
				if (!ExecEHJScanOuterBucket(node, econtext,
											node->hj_CurHashValue))
				{
					/* Outer bucket exhausted; return to Phase 2 driver. */
					node->hj_CurTuple = NULL;
					node->hj_JoinState = HJ_EHJ_PHASE2_LOOP;
					break;
				}
	
				if (node->js.jointype == JOIN_ANTI)
				{
					node->hj_JoinState = HJ_EHJ_PHASE2_LOOP;
					node->hj_CurTuple = NULL;
					break;
				}
				if (node->js.single_match)
				{
					node->hj_JoinState = HJ_EHJ_PHASE2_LOOP;
					node->hj_CurTuple = NULL;
				}
				if (otherqual == NULL || ExecQual(otherqual, econtext))
					return ExecProject(node->js.ps.ps_ProjInfo);
				else
					InstrCountFiltered2(node, 1);
				break;
			}
	
			case HJ_EHJ_PHASE2_SCAN_INNER:
			{
				/*
				* An outer (S) tuple was just inserted in PHASE2_LOOP.
				* Scan the inner bucket for matches.
				*
				* econtext->ecxt_outertuple = inserted S tuple (hj_OuterTupleSlot)
				* Scan sets econtext->ecxt_innertuple on each match.
				*/
				if (!ExecEHJScanInnerBucket(node, econtext,
											node->hj_CurHashValue))
				{
					node->hj_CurTuple = NULL;
					node->hj_JoinState = HJ_EHJ_PHASE2_LOOP;
					break;
				}
	
				if (node->js.jointype == JOIN_ANTI)
				{
					node->hj_JoinState = HJ_EHJ_PHASE2_LOOP;
					node->hj_CurTuple = NULL;
					break;
				}
				if (node->js.single_match)
				{
					node->hj_JoinState = HJ_EHJ_PHASE2_LOOP;
					node->hj_CurTuple = NULL;
				}
				if (otherqual == NULL || ExecQual(otherqual, econtext))
					return ExecProject(node->js.ps.ps_ProjInfo);
				else
					InstrCountFiltered2(node, 1);
				break;
			}
	
			/* --------------------------------------------------------
			* Early Hash Join — Phase 3 (cleanup join)
			* -------------------------------------------------------- */
	
			case HJ_EHJ_PHASE3_NEXT_PART:
			{
				/*
				* Advance to the next partition that has on-disk data on at
				* least one side and load it into memory for the nested-loop
				* cleanup join.
				*
				* A partition is eligible if:
				*   ehj_inner_parts[p].disk_file != NULL  OR
				*   ehj_outer_parts[p].disk_file != NULL
				*
				* We load ALL tuples from both sides (even the in-memory side
				* of an asymmetrically frozen partition) into flat arrays so
				* that the probe loop in PHASE3_PROBE can iterate without
				* re-entering the executor or accessing block chains that may
				* have been freed.
				*/
				HashJoinTable ht = node->hj_HashTable;
				int			n = ht->nbuckets;
	
				/* Free arrays from the previous partition, if any. */
				if (node->ehj_p3_inner_tups != NULL)
				{
					int i;
	
					for (i = 0; i < node->ehj_p3_inner_count; i++)
						if (node->ehj_p3_inner_tups[i])
							pfree(node->ehj_p3_inner_tups[i]);
					pfree(node->ehj_p3_inner_tups);
					pfree(node->ehj_p3_inner_ts);
					pfree(node->ehj_p3_inner_hv);
					node->ehj_p3_inner_tups = NULL;
				}
				if (node->ehj_p3_outer_tups != NULL)
				{
					int i;
	
					for (i = 0; i < node->ehj_p3_outer_count; i++)
						if (node->ehj_p3_outer_tups[i])
							pfree(node->ehj_p3_outer_tups[i]);
					pfree(node->ehj_p3_outer_tups);
					pfree(node->ehj_p3_outer_ts);
					pfree(node->ehj_p3_outer_hv);
					node->ehj_p3_outer_tups = NULL;
				}
	
				/* Scan forward to the next eligible partition. */
				while (node->ehj_p3_partno < n)
				{
					int			p = node->ehj_p3_partno;
					EHJPartData *inner_part = &ht->ehj_inner_parts[p];
					EHJPartData *outer_part = &ht->ehj_outer_parts[p];
	
					node->ehj_p3_partno++;
	
					if (inner_part->disk_file == NULL &&
						outer_part->disk_file == NULL)
						continue;	/* nothing on disk for this partition */
	
					/*
					* Load inner (R) tuples for this partition.
					*
					* Initial capacity estimate: allocate room for 64 tuples
					* and grow by doubling as needed.
					*/
					{
						int			cap = 64;
						int			cnt = 0;
						MinimalTuple *tups = (MinimalTuple *)
							palloc(cap * sizeof(MinimalTuple));
						int64	   *tss = (int64 *) palloc(cap * sizeof(int64));
						uint32	   *hvs = (uint32 *) palloc(cap * sizeof(uint32));
	
						if (inner_part->disk_file != NULL)
						{
							uint32		hv;
							int64		ts;
							MinimalTuple tup;
	
							if (BufFileSeek(inner_part->disk_file, 0, 0L, SEEK_SET))
								ereport(ERROR,
										(errcode_for_file_access(),
										errmsg("could not rewind EHJ inner partition file: %m")));
	
							while (ExecEHJReadNextTuple(inner_part->disk_file,
														&hv, &ts, &tup))
							{
								if (cnt == cap)
								{
									cap *= 2;
									tups = repalloc(tups, cap * sizeof(MinimalTuple));
									tss  = repalloc(tss,  cap * sizeof(int64));
									hvs  = repalloc(hvs,  cap * sizeof(uint32));
								}
								tups[cnt] = tup;
								tss[cnt]  = ts;
								hvs[cnt]  = hv;
								cnt++;
							}
						}
	
						/* Also include any remaining in-memory inner tuples. */
						{
							HashJoinTuple ht_tup =
								ht->buckets.unshared[p];
	
							while (ht_tup != NULL)
							{
								if (cnt == cap)
								{
									cap *= 2;
									tups = repalloc(tups, cap * sizeof(MinimalTuple));
									tss  = repalloc(tss,  cap * sizeof(int64));
									hvs  = repalloc(hvs,  cap * sizeof(uint32));
								}
								/* Copy the tuple; it lives in the (about to be
								* destroyed) batchCxt block chain. */
								MinimalTuple src = EHJ_HJTUPLE_MINTUPLE(ht_tup);
								tups[cnt] = (MinimalTuple) palloc(src->t_len);
								memcpy(tups[cnt], src, src->t_len);
								tss[cnt]  = EHJ_HJTUPLE_ARRIVAL_TS(ht_tup);
								hvs[cnt]  = ht_tup->hashvalue;
								cnt++;
								ht_tup = ht_tup->next.unshared;
							}
						}
	
						node->ehj_p3_inner_tups  = tups;
						node->ehj_p3_inner_ts    = tss;
						node->ehj_p3_inner_hv    = hvs;
						node->ehj_p3_inner_count = cnt;
					}
	
					/* Load outer (S) tuples for this partition. */
					{
						int			cap = 64;
						int			cnt = 0;
						MinimalTuple *tups = (MinimalTuple *)
							palloc(cap * sizeof(MinimalTuple));
						int64	   *tss = (int64 *) palloc(cap * sizeof(int64));
						uint32	   *hvs = (uint32 *) palloc(cap * sizeof(uint32));
	
						if (outer_part->disk_file != NULL)
						{
							uint32		hv;
							int64		ts;
							MinimalTuple tup;
	
							if (BufFileSeek(outer_part->disk_file, 0, 0L, SEEK_SET))
								ereport(ERROR,
										(errcode_for_file_access(),
										errmsg("could not rewind EHJ outer partition file: %m")));
	
							while (ExecEHJReadNextTuple(outer_part->disk_file,
														&hv, &ts, &tup))
							{
								if (cnt == cap)
								{
									cap *= 2;
									tups = repalloc(tups, cap * sizeof(MinimalTuple));
									tss  = repalloc(tss,  cap * sizeof(int64));
									hvs  = repalloc(hvs,  cap * sizeof(uint32));
								}
								tups[cnt] = tup;
								tss[cnt]  = ts;
								hvs[cnt]  = hv;
								cnt++;
							}
						}
	
						/* Also include any remaining in-memory outer tuples. */
						{
							HashJoinTuple ht_tup = ht->outer_buckets[p];
	
							while (ht_tup != NULL)
							{
								if (cnt == cap)
								{
									cap *= 2;
									tups = repalloc(tups, cap * sizeof(MinimalTuple));
									tss  = repalloc(tss,  cap * sizeof(int64));
									hvs  = repalloc(hvs,  cap * sizeof(uint32));
								}
								MinimalTuple src = EHJ_HJTUPLE_MINTUPLE(ht_tup);
								tups[cnt] = (MinimalTuple) palloc(src->t_len);
								memcpy(tups[cnt], src, src->t_len);
								tss[cnt]  = EHJ_HJTUPLE_ARRIVAL_TS(ht_tup);
								hvs[cnt]  = ht_tup->hashvalue;
								cnt++;
								ht_tup = ht_tup->next.unshared;
							}
						}
	
						node->ehj_p3_outer_tups  = tups;
						node->ehj_p3_outer_ts    = tss;
						node->ehj_p3_outer_hv    = hvs;
						node->ehj_p3_outer_count = cnt;
					}
	
					/* Close the disk files — we've loaded everything. */
					if (inner_part->disk_file)
					{
						BufFileClose(inner_part->disk_file);
						inner_part->disk_file = NULL;
					}
					if (outer_part->disk_file)
					{
						BufFileClose(outer_part->disk_file);
						outer_part->disk_file = NULL;
					}
	
					/* Start the nested-loop probe at (ri=0, si=0). */
					node->ehj_p3_ri = 0;
					node->ehj_p3_si = 0;
					node->hj_JoinState = HJ_EHJ_PHASE3_PROBE;
	
					elog(DEBUG1,
						"EHJ Phase 3: partition %d — %d inner, %d outer tuples",
						p,
						node->ehj_p3_inner_count,
						node->ehj_p3_outer_count);
	
					break;			/* re-enter the for(;;) to hit PROBE */
				}
	
				if (node->ehj_p3_partno >= n &&
					node->hj_JoinState == HJ_EHJ_PHASE3_NEXT_PART)
				{
					/*
					* All partitions processed.  The join is complete.
					* Destroy the hash table and signal end of output.
					*/
					ExecHashTableDestroy(node->hj_HashTable);
					node->hj_HashTable = NULL;
					elog(INFO, "EHJ Status: Phase 3 cleanup complete. Join finished.");
					return NULL;
				}
				continue;
			}
	
			case HJ_EHJ_PHASE3_PROBE:
			{
				/*
				* Nested-loop join over the flat arrays loaded by PHASE3_NEXT_PART.
				*
				* Outer loop: inner (R) tuples, indexed by ehj_p3_ri.
				* Inner loop: outer (S) tuples, indexed by ehj_p3_si.
				*
				* For each pair we check:
				*   1. Hash-value equality  (same bucket → same hash? no — but the
				*      join clause covers key equality, so we skip the hv check
				*      and rely on the hash clauses to filter.  We do check hv
				*      equality first as a cheap pre-filter matching the normal
				*      ExecScanHashBucket behaviour.)
				*   2. EHJShouldEmit timestamp check (duplicate avoidance).
				*   3. hashclauses (join key equality).
				*   4. joinqual and otherqual.
				*/
				HashJoinTable ht = node->hj_HashTable;
				int			ri = node->ehj_p3_ri;
				int			si = node->ehj_p3_si;
				int			ri_count = node->ehj_p3_inner_count;
				int			si_count = node->ehj_p3_outer_count;
	
				while (ri < ri_count)
				{
					MinimalTuple r_tup = node->ehj_p3_inner_tups[ri];
					int64		tr_ts  = node->ehj_p3_inner_ts[ri];
					uint32		r_hv   = node->ehj_p3_inner_hv[ri];
	
					/* Determine flush timestamps for this partition.
					* ehj_p3_partno was already advanced; the partition index
					* we are probing is (ehj_p3_partno - 1). */
					int			p      = node->ehj_p3_partno - 1;
					EHJPartData *ipart = &ht->ehj_inner_parts[p];
					EHJPartData *opart = &ht->ehj_outer_parts[p];
	
					while (si < si_count)
					{
						MinimalTuple s_tup = node->ehj_p3_outer_tups[si];
						int64		ts_ts  = node->ehj_p3_outer_ts[si];
						uint32		s_hv   = node->ehj_p3_outer_hv[si];
	
						si++;
	
						/* Cheap pre-filter: hash values must match. */
						if (r_hv != s_hv)
							continue;
	
						/* Duplicate-detection timestamp check. */
						if (!EHJShouldEmit(tr_ts, ts_ts,
										ipart->flush_ts, opart->flush_ts,
										ipart->is_flushed, opart->is_flushed))
							continue;
	
						/*
						* Load the tuple pair into the expression context and
						* evaluate the hash join clauses (key equality) and any
						* additional join/other quals.
						*/
						ExecStoreMinimalTuple(r_tup,
											node->hj_HashTupleSlot,
											false);	/* do not pfree */
						ExecStoreMinimalTuple(s_tup,
											node->hj_OuterTupleSlot,
											false);
						econtext->ecxt_innertuple = node->hj_HashTupleSlot;
						econtext->ecxt_outertuple = node->hj_OuterTupleSlot;
	
						/* Hash clauses encode key equality. */
						if (joinqual != NULL && !ExecQual(joinqual, econtext))
						{
							InstrCountFiltered1(node, 1);
							continue;
						}
	
						/* Anti-join: matched → skip this R tuple entirely. */
						if (node->js.jointype == JOIN_ANTI)
						{
							si = si_count;	/* break inner loop */
							break;
						}
	
						/* Other (non-hash) quals. */
						if (otherqual != NULL && !ExecQual(otherqual, econtext))
						{
							InstrCountFiltered2(node, 1);
							continue;
						}
	
						/*
						* Found a qualifying pair.  Save loop state before
						* returning so we can resume on the next call.
						*/
						node->ehj_p3_ri = ri;
						node->ehj_p3_si = si;	/* already advanced past this si */
						return ExecProject(node->js.ps.ps_ProjInfo);
					}
	
					ri++;
					si = 0;
				}
	
				/*
				* Exhausted all (ri, si) pairs for this partition.  Transition
				* to load the next one.
				*/
				node->ehj_p3_ri = 0;
				node->ehj_p3_si = 0;
				node->hj_JoinState = HJ_EHJ_PHASE3_NEXT_PART;
				continue;
			}
			default:
				elog(ERROR, "unrecognized hashjoin state: %d",
					 (int) node->hj_JoinState);
		}
	}
}

/* ----------------------------------------------------------------
 *		ExecHashJoin
 *
 *		Parallel-oblivious version.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* return: a tuple or NULL */
ExecHashJoin(PlanState *pstate)
{
	/*
	 * On sufficiently smart compilers this should be inlined with the
	 * parallel-aware branches removed.
	 */
	return ExecHashJoinImpl(pstate, false);
}

/* ----------------------------------------------------------------
 *		ExecParallelHashJoin
 *
 *		Parallel-aware version.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* return: a tuple or NULL */
ExecParallelHashJoin(PlanState *pstate)
{
	/*
	 * On sufficiently smart compilers this should be inlined with the
	 * parallel-oblivious branches removed.
	 */
	return ExecHashJoinImpl(pstate, true);
}

/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
HashJoinState *
ExecInitHashJoin(HashJoin *node, EState *estate, int eflags)
{
	HashJoinState *hjstate;
	Plan	   *outerNode;
	Hash	   *hashNode;
	List	   *lclauses;
	List	   *rclauses;
	List	   *hoperators;
	TupleDesc	outerDesc,
				innerDesc;
	ListCell   *l;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	hjstate = makeNode(HashJoinState);
	hjstate->js.ps.plan = (Plan *) node;
	hjstate->js.ps.state = estate;

	/*
	 * See ExecHashJoinInitializeDSM() and ExecHashJoinInitializeWorker()
	 * where this function may be replaced with a parallel version, if we
	 * managed to launch a parallel query.
	 */
	hjstate->js.ps.ExecProcNode = ExecHashJoin;
	hjstate->js.jointype = node->join.jointype;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hjstate->js.ps);

	/*
	 * initialize child nodes
	 *
	 * Note: we could suppress the REWIND flag for the inner input, which
	 * would amount to betting that the hash will be a single batch.  Not
	 * clear if this would be a win or not.
	 */
	outerNode = outerPlan(node);
	hashNode = (Hash *) innerPlan(node);

	outerPlanState(hjstate) = ExecInitNode(outerNode, estate, eflags);
	outerDesc = ExecGetResultType(outerPlanState(hjstate));
	innerPlanState(hjstate) = ExecInitNode((Plan *) hashNode, estate, eflags);
	innerDesc = ExecGetResultType(innerPlanState(hjstate));

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(estate, &hjstate->js.ps);
	ExecAssignProjectionInfo(&hjstate->js.ps, NULL);

	/*
	 * tuple table initialization
	 */
	hjstate->hj_OuterTupleSlot = ExecInitExtraTupleSlot(estate, outerDesc);

	/*
	 * detect whether we need only consider the first matching inner tuple
	 */
	hjstate->js.single_match = (node->join.inner_unique ||
								node->join.jointype == JOIN_SEMI);

	/* set up null tuples for outer joins, if needed */
	switch (node->join.jointype)
	{
		case JOIN_INNER:
		case JOIN_SEMI:
			break;
		case JOIN_LEFT:
		case JOIN_ANTI:
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate, innerDesc);
			break;
		case JOIN_RIGHT:
			hjstate->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate, outerDesc);
			break;
		case JOIN_FULL:
			hjstate->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate, outerDesc);
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate, innerDesc);
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}

	/*
	 * now for some voodoo.  our temporary tuple slot is actually the result
	 * tuple slot of the Hash node (which is our inner plan).  we can do this
	 * because Hash nodes don't return tuples via ExecProcNode() -- instead
	 * the hash join node uses ExecScanHashBucket() to get at the contents of
	 * the hash table.  -cim 6/9/91
	 */
	{
		HashState  *hashstate = (HashState *) innerPlanState(hjstate);
		TupleTableSlot *slot = hashstate->ps.ps_ResultTupleSlot;

		hjstate->hj_HashTupleSlot = slot;
	}

	/*
	 * initialize child expressions
	 */
	hjstate->js.ps.qual =
		ExecInitQual(node->join.plan.qual, (PlanState *) hjstate);
	hjstate->js.joinqual =
		ExecInitQual(node->join.joinqual, (PlanState *) hjstate);
	hjstate->hashclauses =
		ExecInitQual(node->hashclauses, (PlanState *) hjstate);

	/*
	 * initialize hash-specific info
	 */
	hjstate->hj_HashTable = NULL;
	hjstate->hj_FirstOuterTupleSlot = NULL;

	hjstate->hj_CurHashValue = 0;
	hjstate->hj_CurBucketNo = 0;
	hjstate->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
	hjstate->hj_CurTuple = NULL;

	/*
	 * Deconstruct the hash clauses into outer and inner argument values, so
	 * that we can evaluate those subexpressions separately.  Also make a list
	 * of the hash operator OIDs, in preparation for looking up the hash
	 * functions to use.
	 */
	lclauses = NIL;
	rclauses = NIL;
	hoperators = NIL;
	foreach(l, node->hashclauses)
	{
		OpExpr	   *hclause = lfirst_node(OpExpr, l);

		lclauses = lappend(lclauses, ExecInitExpr(linitial(hclause->args),
												  (PlanState *) hjstate));
		rclauses = lappend(rclauses, ExecInitExpr(lsecond(hclause->args),
												  (PlanState *) hjstate));
		hoperators = lappend_oid(hoperators, hclause->opno);
	}
	hjstate->hj_OuterHashKeys = lclauses;
	hjstate->hj_InnerHashKeys = rclauses;
	hjstate->hj_HashOperators = hoperators;
	/* child Hash node needs to evaluate inner hash keys, too */
	((HashState *) innerPlanState(hjstate))->hashkeys = rclauses;

hjstate->hj_JoinState = HJ_EHJ_SYMMETRIC;
	hjstate->hj_MatchedOuter = false;
	hjstate->hj_OuterNotEmpty = false;
 
	/* Phase 1 reading strategy (1:1 alternate) — Phase 2 will override. */
	hjstate->read_ratio_inner = 1;
	hjstate->read_ratio_outer = 1;
	hjstate->reads_from_inner = 0;
	hjstate->reads_from_outer = 0;
 
	/* Phase 2 pending-probe flags. */
	hjstate->ehj_p2_pending_inner = false;
	hjstate->ehj_p2_pending_outer = false;
 
	/* Phase 3 cleanup state. */
	hjstate->ehj_p3_partno      = 0;
	hjstate->ehj_p3_inner_tups  = NULL;
	hjstate->ehj_p3_inner_ts    = NULL;
	hjstate->ehj_p3_inner_hv    = NULL;
	hjstate->ehj_p3_inner_count = 0;
	hjstate->ehj_p3_outer_tups  = NULL;
	hjstate->ehj_p3_outer_ts    = NULL;
	hjstate->ehj_p3_outer_hv    = NULL;
	hjstate->ehj_p3_outer_count = 0;
	hjstate->ehj_p3_ri          = 0;
	hjstate->ehj_p3_si          = 0;

	return hjstate;
}

/* ----------------------------------------------------------------
 *		ExecEndHashJoin
 *
 *		clean up routine for HashJoin node
 * ----------------------------------------------------------------
 */
void
ExecEndHashJoin(HashJoinState *node)
{
	/*
	 * Free hash table
	 */
	if (node->hj_HashTable)
	{
		ExecHashTableDestroy(node->hj_HashTable);
		node->hj_HashTable = NULL;
	}

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->hj_OuterTupleSlot);
	ExecClearTuple(node->hj_HashTupleSlot);

	/*
	 * clean up subtrees
	 */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));
}

/*
 * ExecHashJoinOuterGetTuple
 *
 *		get the next outer tuple for a parallel oblivious hashjoin: either by
 *		executing the outer plan node in the first pass, or from the temp
 *		files for the hashjoin batches.
 *
 * Returns a null slot if no more outer tuples (within the current batch).
 *
 * On success, the tuple's hash value is stored at *hashvalue --- this is
 * either originally computed, or re-read from the temp file.
 */
static TupleTableSlot *
ExecHashJoinOuterGetTuple(PlanState *outerNode,
						  HashJoinState *hjstate,
						  uint32 *hashvalue)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			curbatch = hashtable->curbatch;
	TupleTableSlot *slot;

	if (curbatch == 0)			/* if it is the first pass */
	{
		/*
		 * Check to see if first outer tuple was already fetched by
		 * ExecHashJoin() and not used yet.
		 */
		slot = hjstate->hj_FirstOuterTupleSlot;
		if (!TupIsNull(slot))
			hjstate->hj_FirstOuterTupleSlot = NULL;
		else
			slot = ExecProcNode(outerNode);

		while (!TupIsNull(slot))
		{
			/*
			 * We have to compute the tuple's hash value.
			 */
			ExprContext *econtext = hjstate->js.ps.ps_ExprContext;

			econtext->ecxt_outertuple = slot;
			if (ExecHashGetHashValue(hashtable, econtext,
									 hjstate->hj_OuterHashKeys,
									 true,	/* outer tuple */
									 HJ_FILL_OUTER(hjstate),
									 hashvalue))
			{
				/* remember outer relation is not empty for possible rescan */
				hjstate->hj_OuterNotEmpty = true;

				return slot;
			}

			/*
			 * That tuple couldn't match because of a NULL, so discard it and
			 * continue with the next one.
			 */
			slot = ExecProcNode(outerNode);
		}
	}
	else if (curbatch < hashtable->nbatch)
	{
		BufFile    *file = hashtable->outerBatchFile[curbatch];

		/*
		 * In outer-join cases, we could get here even though the batch file
		 * is empty.
		 */
		if (file == NULL)
			return NULL;

		slot = ExecHashJoinGetSavedTuple(hjstate,
										 file,
										 hashvalue,
										 hjstate->hj_OuterTupleSlot);
		if (!TupIsNull(slot))
			return slot;
	}

	/* End of this batch */
	return NULL;
}

/*
 * ExecHashJoinOuterGetTuple variant for the parallel case.
 */
static TupleTableSlot *
ExecParallelHashJoinOuterGetTuple(PlanState *outerNode,
								  HashJoinState *hjstate,
								  uint32 *hashvalue)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			curbatch = hashtable->curbatch;
	TupleTableSlot *slot;

	/*
	 * In the Parallel Hash case we only run the outer plan directly for
	 * single-batch hash joins.  Otherwise we have to go to batch files, even
	 * for batch 0.
	 */
	if (curbatch == 0 && hashtable->nbatch == 1)
	{
		slot = ExecProcNode(outerNode);

		while (!TupIsNull(slot))
		{
			ExprContext *econtext = hjstate->js.ps.ps_ExprContext;

			econtext->ecxt_outertuple = slot;
			if (ExecHashGetHashValue(hashtable, econtext,
									 hjstate->hj_OuterHashKeys,
									 true,	/* outer tuple */
									 HJ_FILL_OUTER(hjstate),
									 hashvalue))
				return slot;

			/*
			 * That tuple couldn't match because of a NULL, so discard it and
			 * continue with the next one.
			 */
			slot = ExecProcNode(outerNode);
		}
	}
	else if (curbatch < hashtable->nbatch)
	{
		MinimalTuple tuple;

		tuple = sts_parallel_scan_next(hashtable->batches[curbatch].outer_tuples,
									   hashvalue);
		if (tuple != NULL)
		{
			slot = ExecStoreMinimalTuple(tuple,
										 hjstate->hj_OuterTupleSlot,
										 false);
			return slot;
		}
		else
			ExecClearTuple(hjstate->hj_OuterTupleSlot);
	}

	/* End of this batch */
	return NULL;
}

/*
 * ExecHashJoinNewBatch
 *		switch to a new hashjoin batch
 *
 * Returns true if successful, false if there are no more batches.
 */
static bool
ExecHashJoinNewBatch(HashJoinState *hjstate)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			nbatch;
	int			curbatch;
	BufFile    *innerFile;
	TupleTableSlot *slot;
	uint32		hashvalue;

	nbatch = hashtable->nbatch;
	curbatch = hashtable->curbatch;

	if (curbatch > 0)
	{
		/*
		 * We no longer need the previous outer batch file; close it right
		 * away to free disk space.
		 */
		if (hashtable->outerBatchFile[curbatch])
			BufFileClose(hashtable->outerBatchFile[curbatch]);
		hashtable->outerBatchFile[curbatch] = NULL;
	}
	else						/* we just finished the first batch */
	{
		/*
		 * Reset some of the skew optimization state variables, since we no
		 * longer need to consider skew tuples after the first batch. The
		 * memory context reset we are about to do will release the skew
		 * hashtable itself.
		 */
		hashtable->skewEnabled = false;
		hashtable->skewBucket = NULL;
		hashtable->skewBucketNums = NULL;
		hashtable->nSkewBuckets = 0;
		hashtable->spaceUsedSkew = 0;
	}

	/*
	 * We can always skip over any batches that are completely empty on both
	 * sides.  We can sometimes skip over batches that are empty on only one
	 * side, but there are exceptions:
	 *
	 * 1. In a left/full outer join, we have to process outer batches even if
	 * the inner batch is empty.  Similarly, in a right/full outer join, we
	 * have to process inner batches even if the outer batch is empty.
	 *
	 * 2. If we have increased nbatch since the initial estimate, we have to
	 * scan inner batches since they might contain tuples that need to be
	 * reassigned to later inner batches.
	 *
	 * 3. Similarly, if we have increased nbatch since starting the outer
	 * scan, we have to rescan outer batches in case they contain tuples that
	 * need to be reassigned.
	 */
	curbatch++;
	while (curbatch < nbatch &&
		   (hashtable->outerBatchFile[curbatch] == NULL ||
			hashtable->innerBatchFile[curbatch] == NULL))
	{
		if (hashtable->outerBatchFile[curbatch] &&
			HJ_FILL_OUTER(hjstate))
			break;				/* must process due to rule 1 */
		if (hashtable->innerBatchFile[curbatch] &&
			HJ_FILL_INNER(hjstate))
			break;				/* must process due to rule 1 */
		if (hashtable->innerBatchFile[curbatch] &&
			nbatch != hashtable->nbatch_original)
			break;				/* must process due to rule 2 */
		if (hashtable->outerBatchFile[curbatch] &&
			nbatch != hashtable->nbatch_outstart)
			break;				/* must process due to rule 3 */
		/* We can ignore this batch. */
		/* Release associated temp files right away. */
		if (hashtable->innerBatchFile[curbatch])
			BufFileClose(hashtable->innerBatchFile[curbatch]);
		hashtable->innerBatchFile[curbatch] = NULL;
		if (hashtable->outerBatchFile[curbatch])
			BufFileClose(hashtable->outerBatchFile[curbatch]);
		hashtable->outerBatchFile[curbatch] = NULL;
		curbatch++;
	}

	if (curbatch >= nbatch)
		return false;			/* no more batches */

	hashtable->curbatch = curbatch;

	/*
	 * Reload the hash table with the new inner batch (which could be empty)
	 */
	ExecHashTableReset(hashtable);

	innerFile = hashtable->innerBatchFile[curbatch];

	if (innerFile != NULL)
	{
		if (BufFileSeek(innerFile, 0, 0L, SEEK_SET))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not rewind hash-join temporary file: %m")));

		while ((slot = ExecHashJoinGetSavedTuple(hjstate,
												 innerFile,
												 &hashvalue,
												 hjstate->hj_HashTupleSlot)))
		{
			/*
			 * NOTE: some tuples may be sent to future batches.  Also, it is
			 * possible for hashtable->nbatch to be increased here!
			 */
			ExecHashTableInsert(hashtable, slot, hashvalue);
		}

		/*
		 * after we build the hash table, the inner batch file is no longer
		 * needed
		 */
		BufFileClose(innerFile);
		hashtable->innerBatchFile[curbatch] = NULL;
	}

	/*
	 * Rewind outer batch file (if present), so that we can start reading it.
	 */
	if (hashtable->outerBatchFile[curbatch] != NULL)
	{
		if (BufFileSeek(hashtable->outerBatchFile[curbatch], 0, 0L, SEEK_SET))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not rewind hash-join temporary file: %m")));
	}

	return true;
}

/*
 * Choose a batch to work on, and attach to it.  Returns true if successful,
 * false if there are no more batches.
 */
static bool
ExecParallelHashJoinNewBatch(HashJoinState *hjstate)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			start_batchno;
	int			batchno;

	/*
	 * If we started up so late that the batch tracking array has been freed
	 * already by ExecHashTableDetach(), then we are finished.  See also
	 * ExecParallelHashEnsureBatchAccessors().
	 */
	if (hashtable->batches == NULL)
		return false;

	/*
	 * If we were already attached to a batch, remember not to bother checking
	 * it again, and detach from it (possibly freeing the hash table if we are
	 * last to detach).
	 */
	if (hashtable->curbatch >= 0)
	{
		hashtable->batches[hashtable->curbatch].done = true;
		ExecHashTableDetachBatch(hashtable);
	}

	/*
	 * Search for a batch that isn't done.  We use an atomic counter to start
	 * our search at a different batch in every participant when there are
	 * more batches than participants.
	 */
	batchno = start_batchno =
		pg_atomic_fetch_add_u32(&hashtable->parallel_state->distributor, 1) %
		hashtable->nbatch;
	do
	{
		uint32		hashvalue;
		MinimalTuple tuple;
		TupleTableSlot *slot;

		if (!hashtable->batches[batchno].done)
		{
			SharedTuplestoreAccessor *inner_tuples;
			Barrier    *batch_barrier =
			&hashtable->batches[batchno].shared->batch_barrier;

			switch (BarrierAttach(batch_barrier))
			{
				case PHJ_BATCH_ELECTING:

					/* One backend allocates the hash table. */
					if (BarrierArriveAndWait(batch_barrier,
											 WAIT_EVENT_HASH_BATCH_ELECTING))
						ExecParallelHashTableAlloc(hashtable, batchno);
					/* Fall through. */

				case PHJ_BATCH_ALLOCATING:
					/* Wait for allocation to complete. */
					BarrierArriveAndWait(batch_barrier,
										 WAIT_EVENT_HASH_BATCH_ALLOCATING);
					/* Fall through. */

				case PHJ_BATCH_LOADING:
					/* Start (or join in) loading tuples. */
					ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
					inner_tuples = hashtable->batches[batchno].inner_tuples;
					sts_begin_parallel_scan(inner_tuples);
					while ((tuple = sts_parallel_scan_next(inner_tuples,
														   &hashvalue)))
					{
						slot = ExecStoreMinimalTuple(tuple,
													 hjstate->hj_HashTupleSlot,
													 false);
						ExecParallelHashTableInsertCurrentBatch(hashtable, slot,
																hashvalue);
					}
					sts_end_parallel_scan(inner_tuples);
					BarrierArriveAndWait(batch_barrier,
										 WAIT_EVENT_HASH_BATCH_LOADING);
					/* Fall through. */

				case PHJ_BATCH_PROBING:

					/*
					 * This batch is ready to probe.  Return control to
					 * caller. We stay attached to batch_barrier so that the
					 * hash table stays alive until everyone's finished
					 * probing it, but no participant is allowed to wait at
					 * this barrier again (or else a deadlock could occur).
					 * All attached participants must eventually call
					 * BarrierArriveAndDetach() so that the final phase
					 * PHJ_BATCH_DONE can be reached.
					 */
					ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
					sts_begin_parallel_scan(hashtable->batches[batchno].outer_tuples);
					return true;

				case PHJ_BATCH_DONE:

					/*
					 * Already done.  Detach and go around again (if any
					 * remain).
					 */
					BarrierDetach(batch_barrier);
					hashtable->batches[batchno].done = true;
					hashtable->curbatch = -1;
					break;

				default:
					elog(ERROR, "unexpected batch phase %d",
						 BarrierPhase(batch_barrier));
			}
		}
		batchno = (batchno + 1) % hashtable->nbatch;
	} while (batchno != start_batchno);

	return false;
}

/*
 * ExecHashJoinSaveTuple
 *		save a tuple to a batch file.
 *
 * The data recorded in the file for each tuple is its hash value,
 * then the tuple in MinimalTuple format.
 *
 * Note: it is important always to call this in the regular executor
 * context, not in a shorter-lived context; else the temp file buffers
 * will get messed up.
 */
void
ExecHashJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue,
					  BufFile **fileptr)
{
	BufFile    *file = *fileptr;
	size_t		written;

	if (file == NULL)
	{
		/* First write to this batch file, so open it. */
		file = BufFileCreateTemp(false);
		*fileptr = file;
	}

	written = BufFileWrite(file, (void *) &hashvalue, sizeof(uint32));
	if (written != sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to hash-join temporary file: %m")));

	written = BufFileWrite(file, (void *) tuple, tuple->t_len);
	if (written != tuple->t_len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to hash-join temporary file: %m")));
}

/*
 * ExecHashJoinGetSavedTuple
 *		read the next tuple from a batch file.  Return NULL if no more.
 *
 * On success, *hashvalue is set to the tuple's hash value, and the tuple
 * itself is stored in the given slot.
 */
static TupleTableSlot *
ExecHashJoinGetSavedTuple(HashJoinState *hjstate,
						  BufFile *file,
						  uint32 *hashvalue,
						  TupleTableSlot *tupleSlot)
{
	uint32		header[2];
	size_t		nread;
	MinimalTuple tuple;

	/*
	 * We check for interrupts here because this is typically taken as an
	 * alternative code path to an ExecProcNode() call, which would include
	 * such a check.
	 */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Since both the hash value and the MinimalTuple length word are uint32,
	 * we can read them both in one BufFileRead() call without any type
	 * cheating.
	 */
	nread = BufFileRead(file, (void *) header, sizeof(header));
	if (nread == 0)				/* end of file */
	{
		ExecClearTuple(tupleSlot);
		return NULL;
	}
	if (nread != sizeof(header))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from hash-join temporary file: %m")));
	*hashvalue = header[0];
	tuple = (MinimalTuple) palloc(header[1]);
	tuple->t_len = header[1];
	nread = BufFileRead(file,
						(void *) ((char *) tuple + sizeof(uint32)),
						header[1] - sizeof(uint32));
	if (nread != header[1] - sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from hash-join temporary file: %m")));
	return ExecStoreMinimalTuple(tuple, tupleSlot, true);
}


void
ExecReScanHashJoin(HashJoinState *node)
{
	/*
	 * In a multi-batch join, we currently have to do rescans the hard way,
	 * primarily because batch temp files may have already been released. But
	 * if it's a single-batch join, and there is no parameter change for the
	 * inner subnode, then we can just re-use the existing hash table without
	 * rebuilding it.
	 */
	if (node->hj_HashTable != NULL)
	{
		if (node->hj_HashTable->nbatch == 1 &&
			node->js.ps.righttree->chgParam == NULL)
		{
			/*
			 * Okay to reuse the hash table; needn't rescan inner, either.
			 *
			 * However, if it's a right/full join, we'd better reset the
			 * inner-tuple match flags contained in the table.
			 */
			if (HJ_FILL_INNER(node))
				ExecHashTableResetMatchFlags(node->hj_HashTable);

			/*
			 * Also, we need to reset our state about the emptiness of the
			 * outer relation, so that the new scan of the outer will update
			 * it correctly if it turns out to be empty this time. (There's no
			 * harm in clearing it now because ExecHashJoin won't need the
			 * info.  In the other cases, where the hash table doesn't exist
			 * or we are destroying it, we leave this state alone because
			 * ExecHashJoin will need it the first time through.)
			 */
			node->hj_OuterNotEmpty = false;

			/* ExecHashJoin can skip the BUILD_HASHTABLE step */
			node->hj_JoinState = HJ_NEED_NEW_OUTER;
		}
		else
		{
			/* must destroy and rebuild hash table */
			ExecHashTableDestroy(node->hj_HashTable);
			node->hj_HashTable = NULL;
			node->hj_JoinState = HJ_BUILD_HASHTABLE;

			/*
			 * if chgParam of subnode is not null then plan will be re-scanned
			 * by first ExecProcNode.
			 */
			if (node->js.ps.righttree->chgParam == NULL)
				ExecReScan(node->js.ps.righttree);
		}
	}

	/* Always reset intra-tuple state */
	node->hj_CurHashValue = 0;
	node->hj_CurBucketNo = 0;
	node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
	node->hj_CurTuple = NULL;

	node->hj_MatchedOuter = false;
	node->hj_FirstOuterTupleSlot = NULL;

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->js.ps.lefttree->chgParam == NULL)
		ExecReScan(node->js.ps.lefttree);
}

void
ExecShutdownHashJoin(HashJoinState *node)
{
	if (node->hj_HashTable)
	{
		/*
		 * Detach from shared state before DSM memory goes away.  This makes
		 * sure that we don't have any pointers into DSM memory by the time
		 * ExecEndHashJoin runs.
		 */
		ExecHashTableDetachBatch(node->hj_HashTable);
		ExecHashTableDetach(node->hj_HashTable);
	}
}

static void
ExecParallelHashJoinPartitionOuter(HashJoinState *hjstate)
{
	PlanState  *outerState = outerPlanState(hjstate);
	ExprContext *econtext = hjstate->js.ps.ps_ExprContext;
	HashJoinTable hashtable = hjstate->hj_HashTable;
	TupleTableSlot *slot;
	uint32		hashvalue;
	int			i;

	Assert(hjstate->hj_FirstOuterTupleSlot == NULL);

	/* Execute outer plan, writing all tuples to shared tuplestores. */
	for (;;)
	{
		slot = ExecProcNode(outerState);
		if (TupIsNull(slot))
			break;
		econtext->ecxt_outertuple = slot;
		if (ExecHashGetHashValue(hashtable, econtext,
								 hjstate->hj_OuterHashKeys,
								 true,	/* outer tuple */
								 HJ_FILL_OUTER(hjstate),
								 &hashvalue))
		{
			int			batchno;
			int			bucketno;

			ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno,
									  &batchno);
			sts_puttuple(hashtable->batches[batchno].outer_tuples,
						 &hashvalue, ExecFetchSlotMinimalTuple(slot));
		}
		CHECK_FOR_INTERRUPTS();
	}

	/* Make sure all outer partitions are readable by any backend. */
	for (i = 0; i < hashtable->nbatch; ++i)
		sts_end_write(hashtable->batches[i].outer_tuples);
}

void
ExecHashJoinEstimate(HashJoinState *state, ParallelContext *pcxt)
{
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelHashJoinState));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

void
ExecHashJoinInitializeDSM(HashJoinState *state, ParallelContext *pcxt)
{
	int			plan_node_id = state->js.ps.plan->plan_node_id;
	HashState  *hashNode;
	ParallelHashJoinState *pstate;

	/*
	 * Disable shared hash table mode if we failed to create a real DSM
	 * segment, because that means that we don't have a DSA area to work with.
	 */
	if (pcxt->seg == NULL)
		return;

	ExecSetExecProcNode(&state->js.ps, ExecParallelHashJoin);

	/*
	 * Set up the state needed to coordinate access to the shared hash
	 * table(s), using the plan node ID as the toc key.
	 */
	pstate = shm_toc_allocate(pcxt->toc, sizeof(ParallelHashJoinState));
	shm_toc_insert(pcxt->toc, plan_node_id, pstate);

	/*
	 * Set up the shared hash join state with no batches initially.
	 * ExecHashTableCreate() will prepare at least one later and set nbatch
	 * and space_allowed.
	 */
	pstate->nbatch = 0;
	pstate->space_allowed = 0;
	pstate->batches = InvalidDsaPointer;
	pstate->old_batches = InvalidDsaPointer;
	pstate->nbuckets = 0;
	pstate->growth = PHJ_GROWTH_OK;
	pstate->chunk_work_queue = InvalidDsaPointer;
	pg_atomic_init_u32(&pstate->distributor, 0);
	pstate->nparticipants = pcxt->nworkers + 1;
	pstate->total_tuples = 0;
	LWLockInitialize(&pstate->lock,
					 LWTRANCHE_PARALLEL_HASH_JOIN);
	BarrierInit(&pstate->build_barrier, 0);
	BarrierInit(&pstate->grow_batches_barrier, 0);
	BarrierInit(&pstate->grow_buckets_barrier, 0);

	/* Set up the space we'll use for shared temporary files. */
	SharedFileSetInit(&pstate->fileset, pcxt->seg);

	/* Initialize the shared state in the hash node. */
	hashNode = (HashState *) innerPlanState(state);
	hashNode->parallel_state = pstate;
}

/* ----------------------------------------------------------------
 *		ExecHashJoinReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecHashJoinReInitializeDSM(HashJoinState *state, ParallelContext *cxt)
{
	int			plan_node_id = state->js.ps.plan->plan_node_id;
	ParallelHashJoinState *pstate =
	shm_toc_lookup(cxt->toc, plan_node_id, false);

	/*
	 * It would be possible to reuse the shared hash table in single-batch
	 * cases by resetting and then fast-forwarding build_barrier to
	 * PHJ_BUILD_DONE and batch 0's batch_barrier to PHJ_BATCH_PROBING, but
	 * currently shared hash tables are already freed by now (by the last
	 * participant to detach from the batch).  We could consider keeping it
	 * around for single-batch joins.  We'd also need to adjust
	 * finalize_plan() so that it doesn't record a dummy dependency for
	 * Parallel Hash nodes, preventing the rescan optimization.  For now we
	 * don't try.
	 */

	/* Detach, freeing any remaining shared memory. */
	if (state->hj_HashTable != NULL)
	{
		ExecHashTableDetachBatch(state->hj_HashTable);
		ExecHashTableDetach(state->hj_HashTable);
	}

	/* Clear any shared batch files. */
	SharedFileSetDeleteAll(&pstate->fileset);

	/* Reset build_barrier to PHJ_BUILD_ELECTING so we can go around again. */
	BarrierInit(&pstate->build_barrier, 0);
}

void
ExecHashJoinInitializeWorker(HashJoinState *state,
							 ParallelWorkerContext *pwcxt)
{
	HashState  *hashNode;
	int			plan_node_id = state->js.ps.plan->plan_node_id;
	ParallelHashJoinState *pstate =
	shm_toc_lookup(pwcxt->toc, plan_node_id, false);

	/* Attach to the space for shared temporary files. */
	SharedFileSetAttach(&pstate->fileset, pwcxt->seg);

	/* Attach to the shared state in the hash node. */
	hashNode = (HashState *) innerPlanState(state);
	hashNode->parallel_state = pstate;

	ExecSetExecProcNode(&state->js.ps, ExecParallelHashJoin);
}