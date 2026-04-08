/*-------------------------------------------------------------------------
 *
 * hashjoin.h
 *	  internal structures for hash joins
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/hashjoin.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HASHJOIN_H
#define HASHJOIN_H

#include "nodes/execnodes.h"
#include "port/atomics.h"
#include "storage/barrier.h"
#include "storage/buffile.h"
#include "storage/lwlock.h"

/* ----------------------------------------------------------------
 *				hash-join hash table structures
 *
 * Each active hashjoin has a HashJoinTable control block, which is
 * palloc'd in the executor's per-query context.  All other storage needed
 * for the hashjoin is kept in private memory contexts, two for each hashjoin.
 * This makes it easy and fast to release the storage when we don't need it
 * anymore.  (Exception: data associated with the temp files lives in the
 * per-query context too, since we always call buffile.c in that context.)
 *
 * The hashtable contexts are made children of the per-query context, ensuring
 * that they will be discarded at end of statement even if the join is
 * aborted early by an error.  (Likewise, any temporary files we make will
 * be cleaned up by the virtual file manager in event of an error.)
 *
 * Storage that should live through the entire join is allocated from the
 * "hashCxt", while storage that is only wanted for the current batch is
 * allocated in the "batchCxt".  By resetting the batchCxt at the end of
 * each batch, we free all the per-batch storage reliably and without tedium.
 *
 * During first scan of inner relation, we get its tuples from executor.
 * If nbatch > 1 then tuples that don't belong in first batch get saved
 * into inner-batch temp files. The same statements apply for the
 * first scan of the outer relation, except we write tuples to outer-batch
 * temp files.  After finishing the first scan, we do the following for
 * each remaining batch:
 *	1. Read tuples from inner batch file, load into hash buckets.
 *	2. Read tuples from outer batch file, match to hash buckets and output.
 *
 * It is possible to increase nbatch on the fly if the in-memory hash table
 * gets too big.  The hash-value-to-batch computation is arranged so that this
 * can only cause a tuple to go into a later batch than previously thought,
 * never into an earlier batch.  When we increase nbatch, we rescan the hash
 * table and dump out any tuples that are now of a later batch to the correct
 * inner batch file.  Subsequently, while reading either inner or outer batch
 * files, we might find tuples that no longer belong to the current batch;
 * if so, we just dump them out to the correct batch file.
 * ----------------------------------------------------------------
 */

/* these are in nodes/execnodes.h: */
/* typedef struct HashJoinTupleData *HashJoinTuple; */
/* typedef struct HashJoinTableData *HashJoinTable; */

typedef struct HashJoinTupleData
{
	/* link to next tuple in same bucket */
	union
	{
		struct HashJoinTupleData *unshared;
		dsa_pointer shared;
	}			next;
	uint32		hashvalue;		/* tuple's hash code */
	/* Tuple data, in MinimalTuple format, follows on a MAXALIGN boundary */
}			HashJoinTupleData;

#define HJTUPLE_OVERHEAD  MAXALIGN(sizeof(HashJoinTupleData))
#define HJTUPLE_MINTUPLE(hjtup)  \
	((MinimalTuple) ((char *) (hjtup) + HJTUPLE_OVERHEAD))

/* ----------------------------------------------------------------
 * EHJ-extended tuple layout
 *
 * The Early Hash Join stores an arrival timestamp between the standard
 * HashJoinTupleData header and the MinimalTuple payload, so that the Phase 3
 * duplicate-detection logic can compare per-tuple timestamps against the
 * partition flush timestamps recorded in EHJPartData.
 *
 * Memory layout of an EHJ tuple inside a partition block:
 *
 *   offset 0                       : HashJoinTupleData (next ptr + hashvalue)
 *   offset HJTUPLE_OVERHEAD        : int64 arrival_ts  (EHJ_TS_SIZE bytes)
 *   offset EHJ_HJTUPLE_OVERHEAD    : MinimalTuple data (t_len bytes)
 *
 * All EHJ insertion and scan functions use the EHJ_* macros below.  The
 * standard HJTUPLE_MINTUPLE() macro must NOT be used on EHJ tuples.
 * ----------------------------------------------------------------
 */
#define EHJ_TS_SIZE              MAXALIGN(sizeof(int64))
#define EHJ_HJTUPLE_OVERHEAD     (HJTUPLE_OVERHEAD + EHJ_TS_SIZE)

/* Read/write the arrival timestamp embedded in an EHJ tuple header. */
#define EHJ_HJTUPLE_ARRIVAL_TS(hjtup) \
	(*((int64 *) ((char *) (hjtup) + HJTUPLE_OVERHEAD)))

/* Get the MinimalTuple payload from an EHJ tuple header. */
#define EHJ_HJTUPLE_MINTUPLE(hjtup) \
	((MinimalTuple) ((char *) (hjtup) + EHJ_HJTUPLE_OVERHEAD))

/*
 * If the outer relation's distribution is sufficiently nonuniform, we attempt
 * to optimize the join by treating the hash values corresponding to the outer
 * relation's MCVs specially.  Inner relation tuples matching these hash
 * values go into the "skew" hashtable instead of the main hashtable, and
 * outer relation tuples with these hash values are matched against that
 * table instead of the main one.  Thus, tuples with these hash values are
 * effectively handled as part of the first batch and will never go to disk.
 * The skew hashtable is limited to SKEW_WORK_MEM_PERCENT of the total memory
 * allowed for the join; while building the hashtables, we decrease the number
 * of MCVs being specially treated if needed to stay under this limit.
 *
 * Note: you might wonder why we look at the outer relation stats for this,
 * rather than the inner.  One reason is that the outer relation is typically
 * bigger, so we get more I/O savings by optimizing for its most common values.
 * Also, for similarly-sized relations, the planner prefers to put the more
 * uniformly distributed relation on the inside, so we're more likely to find
 * interesting skew in the outer relation.
 */
typedef struct HashSkewBucket
{
	uint32		hashvalue;		/* common hash value */
	HashJoinTuple tuples;		/* linked list of inner-relation tuples */
} HashSkewBucket;

#define SKEW_BUCKET_OVERHEAD  MAXALIGN(sizeof(HashSkewBucket))
#define INVALID_SKEW_BUCKET_NO	(-1)
#define SKEW_WORK_MEM_PERCENT  2
#define SKEW_MIN_OUTER_FRACTION  0.01

/*
 * To reduce palloc overhead, the HashJoinTuples for the current batch are
 * packed in 32kB buffers instead of pallocing each tuple individually.
 */
typedef struct HashMemoryChunkData
{
	int			ntuples;		/* number of tuples stored in this chunk */
	size_t		maxlen;			/* size of the chunk's tuple buffer */
	size_t		used;			/* number of buffer bytes already used */

	/* pointer to the next chunk (linked list) */
	union
	{
		struct HashMemoryChunkData *unshared;
		dsa_pointer shared;
	}			next;

	/*
	 * The chunk's tuple buffer starts after the HashMemoryChunkData struct,
	 * at offset HASH_CHUNK_HEADER_SIZE (which must be maxaligned).  Note that
	 * that offset is not included in "maxlen" or "used".
	 */
}			HashMemoryChunkData;

typedef struct HashMemoryChunkData *HashMemoryChunk;

#define HASH_CHUNK_SIZE			(32 * 1024L)
#define HASH_CHUNK_HEADER_SIZE	MAXALIGN(sizeof(HashMemoryChunkData))
#define HASH_CHUNK_DATA(hc)		(((char *) (hc)) + HASH_CHUNK_HEADER_SIZE)
/* tuples exceeding HASH_CHUNK_THRESHOLD bytes are put in their own chunk */
#define HASH_CHUNK_THRESHOLD	(HASH_CHUNK_SIZE / 4)

/* ----------------------------------------------------------------
 * EHJ Phase 2: partition-exclusive block allocator
 *
 * Standard Postgres HashMemoryChunks interleave tuples from all partitions
 * in a single mixed slab, making per-partition pfree impossible.  The EHJ
 * Phase 2 allocator gives each partition its own chain of contiguous blocks
 * (EHJPartBlock), so that when a partition is flushed to disk the entire
 * block chain can be pfree'd, physically reclaiming that memory.
 *
 * Each EHJPartBlock is a standalone palloc in batchCxt.  Tuples belonging
 * to the same partition are written contiguously within the block; when a
 * block fills, a new one is allocated and linked via EHJPartBlock.next.
 *
 * Memory layout of an EHJPartBlock:
 *
 *   offset 0                          : EHJPartBlockData header
 *   offset EHJ_PART_BLOCK_HEADER_SIZE : packed EHJ tuple data (capacity bytes)
 *
 * ----------------------------------------------------------------
 */
typedef struct EHJPartBlockData
{
	bool		full;			/* true: block is full; allocate a successor */
	size_t		capacity;		/* usable data bytes after the header        */
	size_t		used;			/* bytes already written into data region    */
	struct EHJPartBlockData *next;	/* next block in partition chain, or NULL */
} EHJPartBlockData;

typedef EHJPartBlockData *EHJPartBlock;

/* Size of each partition block. Matches HASH_CHUNK_SIZE for consistency. */
#define EHJ_PART_BLOCK_SIZE         (32 * 1024L)
#define EHJ_PART_BLOCK_HEADER_SIZE  MAXALIGN(sizeof(EHJPartBlockData))
#define EHJ_PART_BLOCK_DATA(blk)    ((char *)(blk) + EHJ_PART_BLOCK_HEADER_SIZE)

/*
 * Tuples larger than this threshold are given their own dedicated block,
 * matching the HASH_CHUNK_THRESHOLD policy in the standard allocator.
 */
#define EHJ_PART_BLOCK_THRESHOLD    (EHJ_PART_BLOCK_SIZE / 4)

/* ----------------------------------------------------------------
 * EHJ post-freeze write buffer
 *
 * When a new tuple hashes to a partition that has already been frozen and
 * flushed to disk, the partition's block chain no longer exists in memory.
 * Instead of re-allocating blocks for it, the tuple is appended to a small
 * in-memory buffer (EHJBufferedTuple array) and bulk-written to the
 * partition's BufFile once the buffer fills or Phase 2 ends.
 *
 * The tuple copy is palloc'd in hashCxt so it survives batchCxt resets.
 * ----------------------------------------------------------------
 */
typedef struct EHJBufferedTuple
{
	uint32		hashvalue;		/* hash value (needed for Phase 3 re-read)  */
	int64		arrival_ts;		/* arrival timestamp at buffering time      */
	MinimalTuple tuple;			/* palloc'd copy of the tuple in hashCxt    */
} EHJBufferedTuple;

/* Initial capacity of a partition's post-freeze write buffer (entries). */
#define EHJ_PART_BUFFER_INITIAL  64

/* ----------------------------------------------------------------
 * EHJ per-partition metadata  (EHJPartData)
 *
 * One EHJPartData struct tracks the state of a single hash bucket/partition
 * on one side of the EHJ join.  Arrays of these structs are allocated in
 * hashCxt (so they survive batchCxt resets), one array per join side:
 *
 *   hashtable->ehj_inner_parts[0 .. nbuckets-1]   inner (R) side
 *   hashtable->ehj_outer_parts[0 .. nbuckets-1]   outer (S) side
 *
 * Memory accounting summary:
 *   - Insertion:  hashtable->spaceUsed += EHJ_HJTUPLE_OVERHEAD + t_len
 *                 part->nbytes         += same
 *   - Block alloc: no spaceUsed change (consistent with dense_alloc policy)
 *   - Flush:      hashtable->spaceUsed -= part->nbytes   (tuple bytes freed)
 *                 pfree each block      (physical reclamation)
 *   - Buf alloc:  hashtable->spaceUsed += buffer_capacity * sizeof(EHJBufferedTuple)
 *   - Buf entry:  hashtable->spaceUsed += tuple->t_len   (palloc'd copy)
 *                 part->buffer_nbytes  += tuple->t_len
 *   - Buf flush:  hashtable->spaceUsed -= part->buffer_nbytes
 *                 pfree each copy; reset buffer_count and buffer_nbytes
 * ----------------------------------------------------------------
 */
typedef struct EHJPartData
{
	/* ── frozen / flushed state ─────────────────────────────────── */
	bool		is_flushed;		/* true once this partition has been frozen  */
	int64		flush_ts;		/* ehj_current_tick at the moment of freeze  */
	BufFile    *disk_file;		/* spill file; NULL until first flush        */

	/* ── in-memory tuple accounting ─────────────────────────────── */
	int			ntuples;		/* live in-memory tuples (0 when flushed)    */
	Size		nbytes;			/* sum of (EHJ_HJTUPLE_OVERHEAD + t_len)    *
								 * for each in-memory tuple; used for victim *
								 * selection and spaceUsed adjustment        */

	/* ── partition-exclusive block chain (batchCxt) ─────────────── */
	EHJPartBlock head_block;	/* first block in chain (for traversal/free) */
	EHJPartBlock tail_block;	/* current write target                      */
	int			nblocks;		/* number of blocks currently allocated      */

	/* ── post-freeze spill buffer (hashCxt; NULL until first freeze) */
	EHJBufferedTuple *buffer;	/* dynamically grown array                   */
	int			buffer_count;	/* number of entries currently in buffer     */
	int			buffer_capacity;/* allocated capacity of buffer array        */
	Size		buffer_nbytes;	/* sum of t_len for all buffered tuple copies*/
} EHJPartData;

typedef EHJPartData *EHJPart;

/*
 * For each batch of a Parallel Hash Join, we have a ParallelHashJoinBatch
 * object in shared memory to coordinate access to it.  Since they are
 * followed by variable-sized objects, they are arranged in contiguous memory
 * but not accessed directly as an array.
 */
typedef struct ParallelHashJoinBatch
{
	dsa_pointer buckets;		/* array of hash table buckets */
	Barrier		batch_barrier;	/* synchronization for joining this batch */

	dsa_pointer chunks;			/* chunks of tuples loaded */
	size_t		size;			/* size of buckets + chunks in memory */
	size_t		estimated_size; /* size of buckets + chunks while writing */
	size_t		ntuples;		/* number of tuples loaded */
	size_t		old_ntuples;	/* number of tuples before repartitioning */
	bool		space_exhausted;

	/*
	 * Variable-sized SharedTuplestore objects follow this struct in memory.
	 * See the accessor macros below.
	 */
} ParallelHashJoinBatch;

/* Accessor for inner batch tuplestore following a ParallelHashJoinBatch. */
#define ParallelHashJoinBatchInner(batch)								\
	((SharedTuplestore *)												\
	 ((char *) (batch) + MAXALIGN(sizeof(ParallelHashJoinBatch))))

/* Accessor for outer batch tuplestore following a ParallelHashJoinBatch. */
#define ParallelHashJoinBatchOuter(batch, nparticipants) \
	((SharedTuplestore *)												\
	 ((char *) ParallelHashJoinBatchInner(batch) +						\
	  MAXALIGN(sts_estimate(nparticipants))))

/* Total size of a ParallelHashJoinBatch and tuplestores. */
#define EstimateParallelHashJoinBatch(hashtable)						\
	(MAXALIGN(sizeof(ParallelHashJoinBatch)) +							\
	 MAXALIGN(sts_estimate((hashtable)->parallel_state->nparticipants)) * 2)

/* Accessor for the nth ParallelHashJoinBatch given the base. */
#define NthParallelHashJoinBatch(base, n)								\
	((ParallelHashJoinBatch *)											\
	 ((char *) (base) +													\
	  EstimateParallelHashJoinBatch(hashtable) *  (n)))

/*
 * Each backend requires a small amount of per-batch state to interact with
 * each ParallelHashJoinBatch.
 */
typedef struct ParallelHashJoinBatchAccessor
{
	ParallelHashJoinBatch *shared;	/* pointer to shared state */

	/* Per-backend partial counters to reduce contention. */
	size_t		preallocated;	/* pre-allocated space for this backend */
	size_t		ntuples;		/* number of tuples */
	size_t		size;			/* size of partition in memory */
	size_t		estimated_size; /* size of partition on disk */
	size_t		old_ntuples;	/* how many tuples before repartitioning? */
	bool		at_least_one_chunk; /* has this backend allocated a chunk? */

	bool		done;			/* flag to remember that a batch is done */
	SharedTuplestoreAccessor *inner_tuples;
	SharedTuplestoreAccessor *outer_tuples;
} ParallelHashJoinBatchAccessor;

/*
 * While hashing the inner relation, any participant might determine that it's
 * time to increase the number of buckets to reduce the load factor or batches
 * to reduce the memory size.  This is indicated by setting the growth flag to
 * these values.
 */
typedef enum ParallelHashGrowth
{
	/* The current dimensions are sufficient. */
	PHJ_GROWTH_OK,
	/* The load factor is too high, so we need to add buckets. */
	PHJ_GROWTH_NEED_MORE_BUCKETS,
	/* The memory budget would be exhausted, so we need to repartition. */
	PHJ_GROWTH_NEED_MORE_BATCHES,
	/* Repartitioning didn't help last time, so don't try to do that again. */
	PHJ_GROWTH_DISABLED
} ParallelHashGrowth;

/*
 * The shared state used to coordinate a Parallel Hash Join.  This is stored
 * in the DSM segment.
 */
typedef struct ParallelHashJoinState
{
	dsa_pointer batches;		/* array of ParallelHashJoinBatch */
	dsa_pointer old_batches;	/* previous generation during repartition */
	int			nbatch;			/* number of batches now */
	int			old_nbatch;		/* previous number of batches */
	int			nbuckets;		/* number of buckets */
	ParallelHashGrowth growth;	/* control batch/bucket growth */
	dsa_pointer chunk_work_queue;	/* chunk work queue */
	int			nparticipants;
	size_t		space_allowed;
	size_t		total_tuples;	/* total number of inner tuples */
	LWLock		lock;			/* lock protecting the above */

	Barrier		build_barrier;	/* synchronization for the build phases */
	Barrier		grow_batches_barrier;
	Barrier		grow_buckets_barrier;
	pg_atomic_uint32 distributor;	/* counter for load balancing */

	SharedFileSet fileset;		/* space for shared temporary files */
} ParallelHashJoinState;

/* The phases for building batches, used by build_barrier. */
#define PHJ_BUILD_ELECTING				0
#define PHJ_BUILD_ALLOCATING			1
#define PHJ_BUILD_HASHING_INNER			2
#define PHJ_BUILD_HASHING_OUTER			3
#define PHJ_BUILD_DONE					4

/* The phases for probing each batch, used by for batch_barrier. */
#define PHJ_BATCH_ELECTING				0
#define PHJ_BATCH_ALLOCATING			1
#define PHJ_BATCH_LOADING				2
#define PHJ_BATCH_PROBING				3
#define PHJ_BATCH_DONE					4

/* The phases of batch growth while hashing, for grow_batches_barrier. */
#define PHJ_GROW_BATCHES_ELECTING		0
#define PHJ_GROW_BATCHES_ALLOCATING		1
#define PHJ_GROW_BATCHES_REPARTITIONING 2
#define PHJ_GROW_BATCHES_DECIDING		3
#define PHJ_GROW_BATCHES_FINISHING		4
#define PHJ_GROW_BATCHES_PHASE(n)		((n) % 5)	/* circular phases */

/* The phases of bucket growth while hashing, for grow_buckets_barrier. */
#define PHJ_GROW_BUCKETS_ELECTING		0
#define PHJ_GROW_BUCKETS_ALLOCATING		1
#define PHJ_GROW_BUCKETS_REINSERTING	2
#define PHJ_GROW_BUCKETS_PHASE(n)		((n) % 3)	/* circular phases */

typedef struct HashJoinTableData
{
	int			nbuckets;		/* # buckets in the in-memory hash table */
	int			log2_nbuckets;	/* its log2 (nbuckets must be a power of 2) */

	int			nbuckets_original;	/* # buckets when starting the first hash */
	int			nbuckets_optimal;	/* optimal # buckets (per batch) */
	int			log2_nbuckets_optimal;	/* log2(nbuckets_optimal) */

	/*
	 * buckets[i] is head of list of tuples in i'th in-memory bucket.
	 *
	 * For Early Hash Join (EHJ), this array has 2 * nbuckets entries.
	 * Buckets [0, nbuckets) hold inner-relation (R) tuples; buckets
	 * [nbuckets, 2*nbuckets) hold outer-relation (S) tuples.  Both sides
	 * share the same memory budget (spaceUsed / spaceAllowed) so that the
	 * Phase 2 biased-flush policy can reclaim memory from whichever side is
	 * larger without any separate accounting.
	 *
	 * In the non-EHJ path the array has exactly nbuckets entries and
	 * outer_buckets is set to NULL (the outer relation is streamed, not
	 * stored).
	 */
	union
	{
		/* unshared array is per-batch storage, as are all the tuples */
		struct HashJoinTupleData **unshared;
		/* shared array is per-query DSA area, as are all the tuples */
		dsa_pointer_atomic *shared;
	}			buckets;

	/*
	 * EHJ only: pointer into the second half of the buckets array, i.e.
	 * &buckets.unshared[nbuckets].  NULL when not running EHJ.
	 * Having a named pointer makes the probe loops readable without
	 * arithmetic at every call site.
	 */
	struct HashJoinTupleData **outer_buckets; /* EHJ outer-rel bucket heads */

	/*
	 * EHJ phase-1 state flags.
	 *   ehj_phase1_done  – set when memory fills or both iterators are
	 *                      exhausted; causes the driver loop to exit the
	 *                      symmetric phase and fall through to standard
	 *                      batch processing (Phase 2, not yet implemented).
	 *   ehj_inner_done   – inner (R) iterator exhausted during phase 1.
	 *   ehj_outer_done   – outer (S) iterator exhausted during phase 1.
	 */
	bool		ehj_enabled;		/* true iff this table was built for EHJ */
	bool		ehj_phase1_done;	/* phase 1 has ended */
	bool		ehj_inner_done;		/* R iterator exhausted */
	bool		ehj_outer_done;		/* S iterator exhausted */

	/*
	 * EHJ Phase 2: per-partition metadata arrays.
	 *
	 * ehj_inner_parts[bucketno] tracks the state of inner (R) partition
	 * 'bucketno'; ehj_outer_parts[bucketno] tracks outer (S) partition
	 * 'bucketno'.  Both arrays have nbuckets entries and are allocated in
	 * hashCxt so they survive batchCxt resets.
	 *
	 * ehj_current_tick is a monotonically increasing counter advanced once
	 * per tuple inserted (inner or outer) and used as the arrival timestamp
	 * stored in each EHJ tuple header and in EHJPartData.flush_ts.
	 *
	 * These fields are NULL / 0 for non-EHJ hash tables.
	 */
	EHJPartData *ehj_inner_parts;	/* [nbuckets] inner (R) partition state  */
	EHJPartData *ehj_outer_parts;	/* [nbuckets] outer (S) partition state  */
	int64		ehj_current_tick;	/* monotonically increasing arrival clock */

	bool		keepNulls;		/* true to store unmatchable NULL tuples */

	bool		skewEnabled;	/* are we using skew optimization? */
	HashSkewBucket **skewBucket;	/* hashtable of skew buckets */
	int			skewBucketLen;	/* size of skewBucket array (a power of 2!) */
	int			nSkewBuckets;	/* number of active skew buckets */
	int		   *skewBucketNums; /* array indexes of active skew buckets */

	int			nbatch;			/* number of batches */
	int			curbatch;		/* current batch #; 0 during 1st pass */

	int			nbatch_original;	/* nbatch when we started inner scan */
	int			nbatch_outstart;	/* nbatch when we started outer scan */

	bool		growEnabled;	/* flag to shut off nbatch increases */

	double		totalTuples;	/* # tuples obtained from inner plan */
	double		partialTuples;	/* # tuples obtained from inner plan by me */
	double		skewTuples;		/* # tuples inserted into skew tuples */

	/*
	 * These arrays are allocated for the life of the hash join, but only if
	 * nbatch > 1.  A file is opened only when we first write a tuple into it
	 * (otherwise its pointer remains NULL).  Note that the zero'th array
	 * elements never get used, since we will process rather than dump out any
	 * tuples of batch zero.
	 */
	BufFile   **innerBatchFile; /* buffered virtual temp file per batch */
	BufFile   **outerBatchFile; /* buffered virtual temp file per batch */

	/*
	 * Info about the datatype-specific hash functions for the datatypes being
	 * hashed. These are arrays of the same length as the number of hash join
	 * clauses (hash keys).
	 */
	FmgrInfo   *outer_hashfunctions;	/* lookup data for hash functions */
	FmgrInfo   *inner_hashfunctions;	/* lookup data for hash functions */
	bool	   *hashStrict;		/* is each hash join operator strict? */

	Size		spaceUsed;		/* memory space currently used by tuples */
	Size		spaceAllowed;	/* upper limit for space used */
	Size		spacePeak;		/* peak space used */
	Size		spaceUsedSkew;	/* skew hash table's current space usage */
	Size		spaceAllowedSkew;	/* upper limit for skew hashtable */

	MemoryContext hashCxt;		/* context for whole-hash-join storage */
	MemoryContext batchCxt;		/* context for this-batch-only storage */

	/* used for dense allocation of tuples (into linked chunks) */
	HashMemoryChunk chunks;		/* one list for the whole batch */

	/* Shared and private state for Parallel Hash. */
	HashMemoryChunk current_chunk;	/* this backend's current chunk */
	dsa_area   *area;			/* DSA area to allocate memory from */
	ParallelHashJoinState *parallel_state;
	ParallelHashJoinBatchAccessor *batches;
	dsa_pointer current_chunk_shared;
}			HashJoinTableData;

#endif							/* HASHJOIN_H */