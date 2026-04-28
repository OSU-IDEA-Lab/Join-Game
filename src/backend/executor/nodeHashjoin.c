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
 * Gutted and replaced with Early Hash Join implementation. 
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
#define HJ_EHJ_SYMMETRIC          1
#define HJ_EHJ_PHASE2_LOOP        2
#define HJ_EHJ_PHASE3_NEXT_PART   3
#define HJ_EHJ_SCAN_OUTER_BUCKET  4
#define HJ_EHJ_SCAN_INNER_BUCKET  5
#define HJ_EHJ_PHASE2_SCAN_OUTER  6
#define HJ_EHJ_PHASE2_SCAN_INNER  7
#define HJ_EHJ_PHASE3_PROBE       8

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
 
static bool
EHJShouldEmit(int64 tr_ts, int64 ts_ts,
              int64 tsf_r, int64 tsf_s,
              bool r_flushed, bool s_flushed)
{
    /* Case 1: S arrived before flush, R arrived after S flush */
    if (s_flushed && ts_ts <= tsf_s && tr_ts > tsf_s)
        return true;

    /* Case 2: S arrived after flush, S arrived before R flush, R arrived after S */
    if (s_flushed && 
        ts_ts > tsf_s && 
        (!r_flushed || ts_ts <= tsf_r) && 
        tr_ts > ts_ts)
        return true;

    /* Case 3: S arrived after R flush */
    if (r_flushed && ts_ts > tsf_r)
        return true;

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

			/* --------------------------------------------------------
			 * Early Hash Join — Phase 1 states
			 * -------------------------------------------------------- */

			case HJ_EHJ_SYMMETRIC:
			{
				HashState	   *hashNode_ehj = (HashState *) innerPlanState(node);
				PlanState	   *innerScan_ehj = outerPlanState(hashNode_ehj);
				PlanState	   *outerScan_ehj = outerPlanState(node);
				ExprContext    *inner_econtext = hashNode_ehj->ps.ps_ExprContext;
				HashJoinTable	hashtable_ehj = node->hj_HashTable;
				TupleTableSlot *slot;
				MinimalTuple	tup = NULL;
				uint32			hashvalue = 0;
				int				bucketno = 0;
				int				dummy_batchno;

				/* First entry: create the EHJ hash table. */
				if (hashtable_ehj == NULL)
				{
					elog(INFO, "EHJ Status: Starting Phase 1 (Symmetric Ping-Pong) execution.");
					hashtable_ehj = ExecEHJHashTableCreate(
						hashNode_ehj, node->hj_HashOperators, HJ_FILL_INNER(node));
					node->hj_HashTable = hashtable_ehj;
					hashNode_ehj->hashtable = hashtable_ehj;
				}

				/* Fast-forward turn if one relation is exhausted */
				if (hashtable_ehj->ehj_inner_done)
					node->reads_from_inner = 1;
				else if (hashtable_ehj->ehj_outer_done)
					node->reads_from_inner = 0;

				/* THE GATE: Check termination */
				if (hashtable_ehj->ehj_phase1_done || 
				   (hashtable_ehj->ehj_inner_done && hashtable_ehj->ehj_outer_done))
				{
					if (hashtable_ehj->ehj_inner_done && hashtable_ehj->ehj_outer_done)
					{
						ExecEHJFlushAllPartitionBuffers(hashtable_ehj);
						node->ehj_p3_partno = 0;
						node->hj_JoinState = HJ_EHJ_PHASE3_NEXT_PART;
						elog(INFO, "EHJ Status: Phase 1 complete (sources exhausted). Entering Phase 3 cleanup.");
					}
					else
					{
						node->read_ratio_inner = 5;
						node->read_ratio_outer = 1;
						node->reads_from_inner = 0;
						node->reads_from_outer = 0;
						node->hj_JoinState = HJ_EHJ_PHASE2_LOOP;
						elog(INFO, "EHJ Status: Phase 1 complete (memory full). Entering Phase 2.");
					}
					continue;
				}

				/* ---- INNER TURN (R) ---- */
				if (node->reads_from_inner == 0)
				{
					slot = ExecProcNode(innerScan_ehj);
					if (TupIsNull(slot)) {
						hashtable_ehj->ehj_inner_done = true;
						node->reads_from_inner = 1; /* Pass turn to Outer */
						continue;
					}

					hashtable_ehj->ehj_current_tick++;
					tup = ExecCopySlotMinimalTuple(slot);
					inner_econtext->ecxt_innertuple = slot;

					if (ExecHashGetHashValue(hashtable_ehj, inner_econtext,
											  node->hj_InnerHashKeys,
											  false, hashtable_ehj->keepNulls,
											  &hashvalue))
					{
						ExecEHJTableInsertInner(hashtable_ehj, tup, hashvalue);
						pfree(tup);
						
						ExecHashGetBucketAndBatch(hashtable_ehj, hashvalue,
												&bucketno, &dummy_batchno);

						if (hashtable_ehj->spaceUsed >= hashtable_ehj->spaceAllowed)
							hashtable_ehj->ehj_phase1_done = true;

						/* Pass turn to Outer for the NEXT iteration */
						node->reads_from_inner = 1;

						/* Set up Inner probing Outer */
						HashJoinTuple stored = hashtable_ehj->buckets.unshared[bucketno];
						ExecStoreMinimalTuple(EHJ_HJTUPLE_MINTUPLE(stored),
											  node->hj_HashTupleSlot, false);
						econtext->ecxt_innertuple = node->hj_HashTupleSlot;

						node->hj_CurBucketNo = bucketno;
						node->hj_CurHashValue = hashvalue;
						node->hj_CurTuple = NULL;
						node->hj_JoinState = HJ_EHJ_SCAN_OUTER_BUCKET;
						continue;
					}
					pfree(tup);
					node->reads_from_inner = 1; /* Pass turn even if null key */
					continue;
				}
				
				/* ---- OUTER TURN (S) ---- */
				if (node->reads_from_inner == 1)
				{
					slot = ExecProcNode(outerScan_ehj);
					if (TupIsNull(slot)) {
						hashtable_ehj->ehj_outer_done = true;
						node->reads_from_inner = 0; /* Pass turn to Inner */
						continue;
					}

					hashtable_ehj->ehj_current_tick++;
					tup = ExecCopySlotMinimalTuple(slot);
					econtext->ecxt_outertuple = slot;

					if (ExecHashGetHashValue(hashtable_ehj, econtext,
											  node->hj_OuterHashKeys,
											  true, HJ_FILL_OUTER(node),
											  &hashvalue))
					{
						ExecEHJTableInsertOuter(hashtable_ehj, tup, hashvalue);
						pfree(tup);

						ExecHashGetBucketAndBatch(hashtable_ehj, hashvalue,
												&bucketno, &dummy_batchno);

						if (hashtable_ehj->spaceUsed >= hashtable_ehj->spaceAllowed)
							hashtable_ehj->ehj_phase1_done = true;

						/* Pass turn to Inner for the NEXT iteration */
						node->reads_from_inner = 0;

						/* Set up Outer probing Inner */
						HashJoinTuple stored = hashtable_ehj->outer_buckets[bucketno];
						ExecStoreMinimalTuple(EHJ_HJTUPLE_MINTUPLE(stored),
											  node->hj_OuterTupleSlot, false);
						econtext->ecxt_outertuple = node->hj_OuterTupleSlot;

						node->hj_CurBucketNo = bucketno;
						node->hj_CurHashValue = hashvalue;
						node->hj_CurTuple = NULL;
						node->hj_JoinState = HJ_EHJ_SCAN_INNER_BUCKET;
						continue;
					}
					pfree(tup);
					node->reads_from_inner = 0; /* Pass turn even if null key */
					continue;
				}
				break;
			}	
			case HJ_EHJ_SCAN_OUTER_BUCKET:
			{
				if (!ExecEHJScanOuterBucket(node, econtext, node->hj_CurHashValue))
				{
					node->hj_CurTuple = NULL;
					node->hj_JoinState = HJ_EHJ_SYMMETRIC;
					break;
				}

				if (node->hashclauses != NULL && !ExecQual(node->hashclauses, econtext))
				{
					InstrCountFiltered1(node, 1);
					break;
				}

				if (joinqual != NULL && !ExecQual(joinqual, econtext))
				{
					InstrCountFiltered1(node, 1);
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
				
				if (otherqual == NULL || ExecQual(otherqual, econtext)) {
					return ExecProject(node->js.ps.ps_ProjInfo);
				} else {
					InstrCountFiltered2(node, 1);
				}
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

				/* Filter out hash collisions */
				if (node->hashclauses != NULL && !ExecQual(node->hashclauses, econtext))
				{
					InstrCountFiltered1(node, 1);
					break;
				}

				/* Match found — verify with joinqual before emitting */
				if (joinqual != NULL && !ExecQual(joinqual, econtext))
				{
					InstrCountFiltered1(node, 1);
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
				if (otherqual == NULL || ExecQual(otherqual, econtext)) {
					return ExecProject(node->js.ps.ps_ProjInfo);
				}
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

					/*
					 * Advance the arrival clock exactly once per tuple read,
					 * here and nowhere else in the call chain.
					 */
					ht->ehj_current_tick++;

				tup = ExecCopySlotMinimalTuple(slot);
				inner_econtext->ecxt_innertuple = slot;
 
				if (!ExecHashGetHashValue(ht, inner_econtext,
										  node->hj_InnerHashKeys,
										  false, ht->keepNulls, &hv))
				{
					/* NULL key — discard and continue cycle. */
					pfree(tup);
					break;
				}
 
				ExecHashGetBucketAndBatch(ht, hv, &bucketno, &dummy_batchno);
 
				if (ht->ehj_inner_parts[bucketno].is_flushed)
				{
					/* The INNER partition is on disk. Buffer it. */
					ExecEHJBufferFrozenTuple(ht,
												&ht->ehj_inner_parts[bucketno],
												tup, hv);
					
					/* FIX: Even though R goes to disk, if S is in memory, it MUST probe S! */
					if (!ht->ehj_outer_parts[bucketno].is_flushed)
					{
						ExecStoreMinimalTuple(tup, node->hj_HashTupleSlot, true); /* slot owns memory */
						econtext->ecxt_innertuple = node->hj_HashTupleSlot;
						node->hj_CurBucketNo = bucketno;
						node->hj_CurHashValue = hv;
						node->hj_CurTuple = NULL;
						node->hj_JoinState = HJ_EHJ_PHASE2_SCAN_OUTER;
					}
					else
					{
						pfree(tup);
					}
				}
				else
				{
					/*
					 * Cases 1 or 3: The INNER partition is in memory.
					 *
					 * Insert the R tuple into its in-memory partition block.
					 * After the insert the bucket head is the new tuple.
					 */
					ExecEHJTableInsertInner(ht, tup, hv);
					pfree(tup);	/* data is now in the block; original no longer needed */
 
					if (!ht->ehj_outer_parts[bucketno].is_flushed)
					{
						/*
						 * Case 1: Both partitions are in memory.
						 *
						 * Probe the outer (S) bucket for existing tuples
						 * that match the R tuple we just inserted.  Point
						 * hj_HashTupleSlot at the block's copy (bucket head).
						 */
						HashJoinTuple stored = ht->buckets.unshared[bucketno];
 
						Assert(stored != NULL);
						ExecStoreMinimalTuple(EHJ_HJTUPLE_MINTUPLE(stored),
											  node->hj_HashTupleSlot,
											  false);	/* block owns memory */
						econtext->ecxt_innertuple = node->hj_HashTupleSlot;
 
						node->hj_CurBucketNo = bucketno;
						node->hj_CurHashValue = hv;
						node->hj_CurTuple = NULL;
						node->hj_JoinState = HJ_EHJ_PHASE2_SCAN_OUTER;
					}
					/*
					 * Case 3: Inner in memory, outer on disk.
					 *
					 * The R tuple is now safely in memory.  The matching S
					 * partition is frozen and cannot be probed in Phase 2.
					 * Phase 3 will load the outer partition from disk and
					 * join it against this in-memory R partition.
					 *
					 * No state transition — fall through to break and let
					 * the Phase 2 loop read the next tuple.
					 */
				}
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

					/*
					 * Advance the arrival clock exactly once per tuple read,
					 * here and nowhere else in the call chain.
					 */
					ht->ehj_current_tick++;

					tup = ExecCopySlotMinimalTuple(slot);
					econtext->ecxt_outertuple = slot;
	
					if (!ExecHashGetHashValue(ht, econtext,
											node->hj_OuterHashKeys,
											true, HJ_FILL_OUTER(node), &hv))
					{
						/* NULL key — discard and reset cycle. */
						pfree(tup);
						node->reads_from_inner = 0;
						node->reads_from_outer = 0;
						break;
					}
	
					ExecHashGetBucketAndBatch(ht, hv, &bucketno, &dummy_batchno);
	
					if (ht->ehj_outer_parts[bucketno].is_flushed)
					{
						/* The OUTER partition is on disk. Buffer it. */
						ExecEHJBufferFrozenTuple(ht,
												&ht->ehj_outer_parts[bucketno],
												tup, hv);
						
						/* FIX: Even though S goes to disk, if R is in memory, it MUST probe R! */
						if (!ht->ehj_inner_parts[bucketno].is_flushed)
						{
							ExecStoreMinimalTuple(tup, node->hj_OuterTupleSlot, true); /* slot owns memory */
							econtext->ecxt_outertuple = node->hj_OuterTupleSlot;
							node->hj_CurBucketNo = bucketno;
							node->hj_CurHashValue = hv;
							node->hj_CurTuple = NULL;
							node->hj_JoinState = HJ_EHJ_PHASE2_SCAN_INNER;
						}
						else
						{
							pfree(tup);
						}
					}
					else
					{
						/*
						* Cases 1 or 3: The OUTER partition is in memory.
						*
						* Insert the S tuple into its in-memory partition block.
						*/
						ExecEHJTableInsertOuter(ht, tup, hv);
						pfree(tup);	/* data is now in the block */
	
						if (!ht->ehj_inner_parts[bucketno].is_flushed)
						{
							/*
							* Case 1: Both partitions are in memory.
							*
							* Probe the inner (R) bucket for matches.
							*/
							HashJoinTuple stored = ht->outer_buckets[bucketno];
	
							Assert(stored != NULL);
							ExecStoreMinimalTuple(EHJ_HJTUPLE_MINTUPLE(stored),
												node->hj_OuterTupleSlot,
												false);	/* block owns memory */
							econtext->ecxt_outertuple = node->hj_OuterTupleSlot;
	
							node->hj_CurBucketNo = bucketno;
							node->hj_CurHashValue = hv;
							node->hj_CurTuple = NULL;
							node->hj_JoinState = HJ_EHJ_PHASE2_SCAN_INNER;
						}
						/*
						* Case 3: Outer in memory, inner on disk.
						*
						* The S tuple is in memory.  The matching R partition is
						* frozen and cannot be probed now.  Phase 3 will load
						* the inner partition from disk and join against the
						* in-memory S partition.
						*/
					}
	
					/* Completed one full A:B cycle; reset counters. */
                    if (node->reads_from_outer >= node->read_ratio_outer)
                    {
                        /* If a relation is done, permanently fast-forward its turn */
                        node->reads_from_inner = ht->ehj_inner_done ? node->read_ratio_inner : 0;
                        node->reads_from_outer = ht->ehj_outer_done ? node->read_ratio_outer : 0;
                    }
                    break;
                }
                else
                {
                    /* Fallback state reset */
                    node->reads_from_inner = ht->ehj_inner_done ? node->read_ratio_inner : 0;
                    node->reads_from_outer = ht->ehj_outer_done ? node->read_ratio_outer : 0;
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
	
				/* Filter out hash collisions */
				if (node->hashclauses != NULL && !ExecQual(node->hashclauses, econtext))
				{
					InstrCountFiltered1(node, 1);
					break;
				}

				/* Match found — verify with joinqual before emitting */
				if (joinqual != NULL && !ExecQual(joinqual, econtext))
				{
					InstrCountFiltered1(node, 1);
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
				if (otherqual == NULL || ExecQual(otherqual, econtext)) {
					return ExecProject(node->js.ps.ps_ProjInfo);
				}
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
	
				/* Filter out hash collisions */
				if (node->hashclauses != NULL && !ExecQual(node->hashclauses, econtext))
				{
					InstrCountFiltered1(node, 1);
					break;
				}

				/* Match found — verify with joinqual before emitting */
				if (joinqual != NULL && !ExecQual(joinqual, econtext))
				{
					InstrCountFiltered1(node, 1);
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
				if (otherqual == NULL || ExecQual(otherqual, econtext)) {
					return ExecProject(node->js.ps.ps_ProjInfo);
				}
				else
					InstrCountFiltered2(node, 1);
				break;
			}
	
			/* --------------------------------------------------------
			* Early Hash Join — Phase 3 (cleanup join)
			* -------------------------------------------------------- */
	
			case HJ_EHJ_PHASE3_NEXT_PART:
			{
				HashJoinTable ht = node->hj_HashTable;
				int			n = ht->nbuckets;
				MemoryContext oldcxt; 

				/* Switch to the Long-lived hash context */
				oldcxt = MemoryContextSwitchTo(ht->hashCxt);
	
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
	
					/* Load inner (R) tuples for this partition. */
					{
						int			cap = 64;
						int			cnt = 0;
						MinimalTuple *tups = (MinimalTuple *) palloc(cap * sizeof(MinimalTuple));
						int64	   *tss = (int64 *) palloc(cap * sizeof(int64));
						uint32	   *hvs = (uint32 *) palloc(cap * sizeof(uint32));

						if (inner_part->disk_file != NULL)
						{
							/* Load the spilled tuples from disk. */
							uint32		hv;
							int64		ts;
							MinimalTuple tup;

							if (BufFileSeek(inner_part->disk_file, 0, 0L, SEEK_SET))
								ereport(ERROR, (errcode_for_file_access(), errmsg("could not rewind EHJ inner partition file: %m")));

							while (ExecEHJReadNextTuple(inner_part->disk_file, &hv, &ts, &tup))
							{
								if (cnt == cap) {
									cap *= 2;
									tups = repalloc(tups, cap * sizeof(MinimalTuple));
									tss  = repalloc(tss,  cap * sizeof(int64));
									hvs  = repalloc(hvs,  cap * sizeof(uint32));
								}
								tups[cnt] = tup; tss[cnt] = ts; hvs[cnt] = hv;
								cnt++;
							}
						}
						
						/* ALWAYS load the pre-flush tuples from memory. */
						{
							HashJoinTuple ht_tup = ht->buckets.unshared[p];
							while (ht_tup != NULL)
							{
								if (cnt == cap) {
									cap *= 2;
									tups = repalloc(tups, cap * sizeof(MinimalTuple));
									tss  = repalloc(tss,  cap * sizeof(int64));
									hvs  = repalloc(hvs,  cap * sizeof(uint32));
								}
								MinimalTuple src = EHJ_HJTUPLE_MINTUPLE(ht_tup);
								tups[cnt] = (MinimalTuple) palloc(src->t_len);
								memcpy(tups[cnt], src, src->t_len);
								tss[cnt] = EHJ_HJTUPLE_ARRIVAL_TS(ht_tup);
								hvs[cnt] = ht_tup->hashvalue;
								cnt++;
								ht_tup = ht_tup->next.unshared;
							}
						}
						node->ehj_p3_inner_tups = tups;
						node->ehj_p3_inner_ts = tss;
						node->ehj_p3_inner_hv = hvs;
						node->ehj_p3_inner_count = cnt;
					}

					/* Load outer (S) tuples for this partition. */
					{
						int			cap = 64;
						int			cnt = 0;
						MinimalTuple *tups = (MinimalTuple *) palloc(cap * sizeof(MinimalTuple));
						int64	   *tss = (int64 *) palloc(cap * sizeof(int64));
						uint32	   *hvs = (uint32 *) palloc(cap * sizeof(uint32));

						if (outer_part->disk_file != NULL)
						{
							/* Load the spilled tuples from disk. */
							uint32		hv;
							int64		ts;
							MinimalTuple tup;

							if (BufFileSeek(outer_part->disk_file, 0, 0L, SEEK_SET))
								ereport(ERROR, (errcode_for_file_access(), errmsg("could not rewind EHJ outer partition file: %m")));

							while (ExecEHJReadNextTuple(outer_part->disk_file, &hv, &ts, &tup))
							{
								if (cnt == cap) {
									cap *= 2;
									tups = repalloc(tups, cap * sizeof(MinimalTuple));
									tss  = repalloc(tss,  cap * sizeof(int64));
									hvs  = repalloc(hvs,  cap * sizeof(uint32));
								}
								tups[cnt] = tup; tss[cnt] = ts; hvs[cnt] = hv;
								cnt++;
							}
						}
						
						/* ALWAYS load the pre-flush tuples from memory. */
						{
							HashJoinTuple ht_tup = ht->outer_buckets[p];
							while (ht_tup != NULL)
							{
								if (cnt == cap) {
									cap *= 2;
									tups = repalloc(tups, cap * sizeof(MinimalTuple));
									tss  = repalloc(tss,  cap * sizeof(int64));
									hvs  = repalloc(hvs,  cap * sizeof(uint32));
								}
								MinimalTuple src = EHJ_HJTUPLE_MINTUPLE(ht_tup);
								tups[cnt] = (MinimalTuple) palloc(src->t_len);
								memcpy(tups[cnt], src, src->t_len);
								tss[cnt] = EHJ_HJTUPLE_ARRIVAL_TS(ht_tup);
								hvs[cnt] = ht_tup->hashvalue;
								cnt++;
								ht_tup = ht_tup->next.unshared;
							}
						}
						node->ehj_p3_outer_tups = tups;
						node->ehj_p3_outer_ts = tss;
						node->ehj_p3_outer_hv = hvs;
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
	
					/* RESTORE THE OLD CONTEXT before breaking out to probe */
					MemoryContextSwitchTo(oldcxt);
					break;			/* break out of the while loop */
				}
	
				if (node->ehj_p3_partno >= n)
				{
					/* RESTORE THE OLD CONTEXT before finishing the join */
					MemoryContextSwitchTo(oldcxt);
					ExecHashTableDestroy(node->hj_HashTable);
					node->hj_HashTable = NULL;
					elog(INFO, "EHJ Status: Phase 3 cleanup complete. Join finished.");
					return NULL;
				}
				
				/* Break out of the switch statement so the driver loops to the probe phase */
				break;
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
						if (node->hashclauses != NULL && !ExecQual(node->hashclauses, econtext))
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
	
						if (joinqual != NULL && !ExecQual(joinqual, econtext))
						{
							InstrCountFiltered1(node, 1);
							continue;
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
	/* * EHJ GUARD:
	 * Early Hash Join does not support rescanning mid-execution. 
	 * If the hash table exists, we are already executing and must abort.
	 */
	if (node->hj_HashTable != NULL)
		elog(ERROR, "Early Hash Join (EHJ) does not currently support rescanning");

	/*
	 * If we reach here, hj_HashTable is NULL. This means the executor 
	 * requested a rescan BEFORE Phase 1 even started.
	 * * We safely reset the baseline intra-tuple state to ensure a clean start.
	 */
	node->hj_JoinState = HJ_EHJ_SYMMETRIC;
	node->hj_CurHashValue = 0;
	node->hj_CurBucketNo = 0;
	node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
	node->hj_CurTuple = NULL;
	node->hj_MatchedOuter = false;
	node->hj_FirstOuterTupleSlot = NULL;

	/*
	 * Pass the rescan command down to the child nodes so they can 
	 * re-evaluate parameters if necessary.
	 */
	if (node->js.ps.righttree->chgParam == NULL)
		ExecReScan(node->js.ps.righttree);

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