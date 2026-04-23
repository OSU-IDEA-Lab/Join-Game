/*-------------------------------------------------------------------------
 *
 * nodeHash.h
 *	  prototypes for nodeHash.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeHash.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEHASH_H
#define NODEHASH_H

#include "access/parallel.h"
#include "nodes/execnodes.h"
#include "executor/hashjoin.h"

struct SharedHashJoinBatch;

extern HashState *ExecInitHash(Hash *node, EState *estate, int eflags);
extern Node *MultiExecHash(HashState *node);
extern void ExecEndHash(HashState *node);
extern void ExecReScanHash(HashState *node);

extern HashJoinTable ExecHashTableCreate(HashState *state, List *hashOperators,
					bool keepNulls);

/* -------------------------------------------------------------------------
 * Early Hash Join (EHJ) — Phase 1 and Phase 2 interface
 *
 * Phase 1 (symmetric in-memory join)
 * ------------------------------------
 * ExecEHJHashTableCreate creates a hash table whose bucket array is twice the
 * normal width: [0, nbuckets) for inner (R) tuples and [nbuckets, 2*nbuckets)
 * for outer (S) tuples.  Both sides share a single spaceUsed counter so that
 * the Phase 2 biased-flush policy can reason about the combined footprint.
 *
 * ExecEHJTableInsertInner / ExecEHJTableInsertOuter insert a tuple into the
 * appropriate partition's block chain, probe the opposite side for matches,
 * and update per-partition metadata (ntuples, nbytes) and ehj_current_tick.
 * If the target partition is already frozen, the tuple is handed to
 * ExecEHJBufferFrozenTuple instead of being inserted.
 *
 * ExecEHJScanInnerBucket / ExecEHJScanOuterBucket are the per-tuple bucket
 * iterators used during the symmetric probe; they mirror ExecScanHashBucket
 * but operate on the named half of the double-wide array and use the
 * EHJ_HJTUPLE_MINTUPLE() macro to account for the arrival-timestamp field.
 *
 * Phase 2 (biased flush / spill)
 * --------------------------------
 * ExecEHJInitPartitions allocates and zero-initialises the ehj_inner_parts
 * and ehj_outer_parts arrays in hashCxt.  Called once by
 * ExecEHJHashTableCreate; must be called before any Phase 2 function.
 *
 * ExecEHJPickVictimOuter / ExecEHJPickVictimInner perform an O(nbuckets)
 * scan over the unfrozen partitions of the named side and return the bucket
 * number of the victim (largest outer, smallest inner), or -1 if none exist.
 *
 * ExecEHJFlushPartition freezes a single partition:
 *   1. Traverses the bucket chain and writes each tuple to disk as
 *      [uint32 hashvalue][int64 arrival_ts][MinimalTuple bytes].
 *   2. NULLs the bucket head pointer.
 *   3. pfree's every block in the partition's EHJPartBlock chain, physically
 *      reclaiming that memory and decrementing hashtable->spaceUsed by
 *      part->nbytes (the previously accounted tuple bytes).
 *   4. Allocates the post-freeze write buffer in hashCxt and charges its
 *      initial footprint to spaceUsed.
 *   5. Sets part->is_flushed = true and records part->flush_ts.
 *
 * ExecEHJBufferFrozenTuple appends a new tuple arriving for a frozen
 * partition to that partition's in-memory buffer.  The MinimalTuple is
 * copied into hashCxt.  When the buffer fills, ExecEHJFlushPartitionBuffer
 * is called automatically.
 *
 * ExecEHJFlushPartitionBuffer bulk-writes all buffered tuples for one
 * partition to its BufFile, then pfrees each MinimalTuple copy and
 * decrements spaceUsed by part->buffer_nbytes.
 *
 * ExecEHJFlushAllPartitionBuffers drains every non-empty post-freeze buffer
 * on both sides before Phase 3 begins.
 *
 * ExecEHJBiasedFlush is the top-level Phase 2 flush driver.  It loops until
 * spaceUsed < spaceAllowed, each iteration picking and flushing the largest
 * unfrozen outer partition (or the smallest unfrozen inner partition if no
 * outer partition remains).  Returns false if memory is still full after
 * exhausting all candidates.
 * ------------------------------------------------------------------------- */

/* ── Phase 1 ─────────────────────────────────────────────────────────── */

extern HashJoinTable ExecEHJHashTableCreate(HashState *state,
					List *hashOperators, bool keepNulls);

extern void ExecEHJTableInsertInner(HashJoinTable hashtable,
					MinimalTuple tuple, uint32 hashvalue);
extern void ExecEHJTableInsertOuter(HashJoinTable hashtable,
					MinimalTuple tuple, uint32 hashvalue);

extern bool ExecEHJScanInnerBucket(HashJoinState *hjstate,
					ExprContext *econtext, uint32 hashvalue);
extern bool ExecEHJScanOuterBucket(HashJoinState *hjstate,
					ExprContext *econtext, uint32 hashvalue);

/* ── Phase 2 ─────────────────────────────────────────────────────────── */

/*
 * ExecEHJInitPartitions
 *		Allocate and zero-initialise the ehj_inner_parts and ehj_outer_parts
 *		arrays (one EHJPartData per bucket, per side) in hashCxt.
 *		Called once from ExecEHJHashTableCreate.
 */
extern void ExecEHJInitPartitions(HashJoinTable hashtable);

/*
 * ExecEHJPickVictimOuter
 *		Return the bucket number of the largest non-empty, non-frozen outer
 *		(S) partition, or -1 if no such partition exists.
 */
extern int	ExecEHJPickVictimOuter(HashJoinTable hashtable);

/*
 * ExecEHJPickVictimInner
 *		Return the bucket number of the smallest non-empty, non-frozen inner
 *		(R) partition, or -1 if no such partition exists.
 */
extern int	ExecEHJPickVictimInner(HashJoinTable hashtable);

/*
 * ExecEHJFlushPartition
 *		Freeze partition 'bucketno' on the named side (is_inner = true → inner
 *		(R) side; false → outer (S) side).
 *
 *		On return: the partition's block chain has been pfree'd, spaceUsed has
 *		been decremented by the tuple bytes that were in the partition, the
 *		bucket head pointer is NULL, the BufFile has been created/extended,
 *		and the post-freeze write buffer has been initialised.
 */
extern void ExecEHJFlushPartition(HashJoinTable hashtable,
					EHJPartData *part, int bucketno, bool is_inner);

/*
 * ExecEHJBufferFrozenTuple
 *		Append a tuple destined for a frozen partition to its write buffer.
 *		Calls ExecEHJFlushPartitionBuffer automatically when the buffer fills.
 *		'arrival_ts' should be the current value of hashtable->ehj_current_tick
 *		at the time the tuple is read.
 */
extern void ExecEHJBufferFrozenTuple(HashJoinTable hashtable,
					EHJPartData *part, MinimalTuple tuple,
					uint32 hashvalue);

/*
 * ExecEHJFlushPartitionBuffer
 *		Write all buffered post-freeze tuples for 'part' to its BufFile,
 *		pfree each MinimalTuple copy, and decrement spaceUsed accordingly.
 *		Resets part->buffer_count and part->buffer_nbytes to 0.
 */
extern void ExecEHJFlushPartitionBuffer(HashJoinTable hashtable,
					EHJPartData *part);

/*
 * ExecEHJFlushAllPartitionBuffers
 *		Drain every non-empty post-freeze buffer on both sides.
 *		Must be called before Phase 3 begins to ensure no tuples are lost.
 */
extern void ExecEHJFlushAllPartitionBuffers(HashJoinTable hashtable);

/*
 * ExecEHJBiasedFlush
 *		Top-level Phase 2 flush driver implementing the biased flushing policy:
 *		  • Flush the largest non-frozen outer (S) partition, or
 *		  • If none, flush the smallest non-frozen inner (R) partition.
 *		Loops until spaceUsed < spaceAllowed or no candidates remain.
 *		Returns true if memory pressure was successfully relieved,
 *		false if all unfrozen partitions have been exhausted.
 */
extern bool ExecEHJBiasedFlush(HashJoinTable hashtable);

/* ── Shared infrastructure ───────────────────────────────────────────── */

extern void ExecParallelHashTableAlloc(HashJoinTable hashtable,
						   int batchno);
extern void ExecHashTableDestroy(HashJoinTable hashtable);
extern void ExecHashTableDetach(HashJoinTable hashtable);
extern void ExecHashTableDetachBatch(HashJoinTable hashtable);
extern void ExecParallelHashTableSetCurrentBatch(HashJoinTable hashtable,
									 int batchno);

extern void ExecHashTableInsert(HashJoinTable hashtable,
					TupleTableSlot *slot,
					uint32 hashvalue);
extern void ExecParallelHashTableInsert(HashJoinTable hashtable,
							TupleTableSlot *slot,
							uint32 hashvalue);
extern void ExecParallelHashTableInsertCurrentBatch(HashJoinTable hashtable,
										TupleTableSlot *slot,
										uint32 hashvalue);
extern bool ExecHashGetHashValue(HashJoinTable hashtable,
					 ExprContext *econtext,
					 List *hashkeys,
					 bool outer_tuple,
					 bool keep_nulls,
					 uint32 *hashvalue);
extern void ExecHashGetBucketAndBatch(HashJoinTable hashtable,
						  uint32 hashvalue,
						  int *bucketno,
						  int *batchno);
extern bool ExecScanHashBucket(HashJoinState *hjstate, ExprContext *econtext);
extern bool ExecParallelScanHashBucket(HashJoinState *hjstate, ExprContext *econtext);
extern void ExecPrepHashTableForUnmatched(HashJoinState *hjstate);
extern bool ExecScanHashTableForUnmatched(HashJoinState *hjstate,
							  ExprContext *econtext);
extern void ExecHashTableReset(HashJoinTable hashtable);
extern void ExecHashTableResetMatchFlags(HashJoinTable hashtable);
extern void ExecChooseHashTableSize(double ntuples, int tupwidth, bool useskew,
						bool try_combined_work_mem,
						int parallel_workers,
						size_t *space_allowed,
						int *numbuckets,
						int *numbatches,
						int *num_skew_mcvs);
extern int	ExecHashGetSkewBucket(HashJoinTable hashtable, uint32 hashvalue);
extern void ExecHashEstimate(HashState *node, ParallelContext *pcxt);
extern void ExecHashInitializeDSM(HashState *node, ParallelContext *pcxt);
extern void ExecHashInitializeWorker(HashState *node, ParallelWorkerContext *pwcxt);
extern void ExecHashRetrieveInstrumentation(HashState *node);
extern void ExecShutdownHash(HashState *node);
extern void ExecHashGetInstrumentation(HashInstrumentation *instrument,
						   HashJoinTable hashtable);

#endif							/* NODEHASH_H */