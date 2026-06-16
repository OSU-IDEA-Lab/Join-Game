# EHJ Modification Notes

The original PostgreSQL hash join (Hybrid Hash Join) has been replaced in-place with an **Early Hash Join (EHJ)** as described in the [Early Hash Join paper](https://cmps-people.ok.ubc.ca/rlawrenc/research/Papers/EarlyHashJoin.pdf).

---

## Original vs. EHJ Architecture

The original Hybrid Hash Join built the entire inner relation into a hash table before reading from the outer relation. EHJ instead reads from both relations **symmetrically and simultaneously**, producing join results early and deferring spill decisions until memory pressure forces them.

---

## Plan and Execution State Trees

### Plan Tree Inheritance

**Node** → **Plan** → **Join** → **HashJoin**

### Execution State Tree Inheritance

**Node** → **PlanState** → **JoinState** → **HashJoinState**

---

## Architectural Decision: Memory Layout for Symmetric Join

### Standard Hash Join: Shared Slab Allocation

The standard Hybrid Hash Join allocates tuple memory from a **single shared slab** (`HashMemoryChunk` chain) that is global to the entire batch. All partitions interleave their tuples into this shared arena:

```
HashMemoryChunkData → [chunk 1 data: R_bucket_3 | R_bucket_7 | R_bucket_1 | ...]
HashMemoryChunkData → [chunk 2 data: R_bucket_3 | R_bucket_9 | ...]
```

This design is simple and efficient for the standard case: at the end of each batch the entire `batchCxt` is reset in one operation, freeing all chunks at once. Individual partition memory cannot be reclaimed mid-batch, but that never mattered because the standard join never needs to — it builds the full inner relation first, then streams the outer relation through. Spilling is handled by deciding upfront (at table creation time) how many batches are needed and routing tuples to batch files; it never requires freeing the memory of one partition while keeping another.

### EHJ: Why Shared Slab Allocation Fails for Symmetric Join

EHJ's Phase 2 changes this constraint fundamentally. Because both relations are being read simultaneously and memory fills during the join (not before it), EHJ must **selectively flush individual partitions to disk** while continuing to hold other partitions in memory. After flushing partition *k*, new tuples for partition *k* are buffered and written to disk, while partitions *k±1, k±2, ...* remain live in memory and continue to receive inserts and participate in probes.

Under a shared slab, the memory bytes belonging to flushed partition *k* are scattered through chunks that also contain tuples for live partitions. There is no way to `pfree` those bytes without freeing the entire chunk and invalidating the live tuples packed alongside them. The result would be that "flushing" a partition to disk would release no memory at all — the slab pages would remain pinned until the whole batch ends.

### EHJ Solution: Partition-Exclusive Block Chains (`EHJPartBlock`)

EHJ gives **each partition its own private linked list of 32 KB blocks** (`EHJPartBlock`). Tuples belonging to a single partition are written contiguously into that partition's block chain and nowhere else:

```
Partition 3 (inner):  [block A: R3_t1 | R3_t2 | R3_t3 ...] → [block B: R3_t4 ...] → NULL
Partition 7 (inner):  [block A: R7_t1 | R7_t2 ...] → NULL
Partition 3 (outer):  [block A: S3_t1 ...] → NULL
```

Each `EHJPartBlock` is a standalone `palloc` in `batchCxt`. When partition *k* is selected for flushing, `ExecEHJFlushPartition` traverses the partition's block chain, writes every tuple to a `BufFile`, and then calls `pfree` on each block individually. Because the blocks are partition-exclusive, no other partition's data is disturbed. `spaceUsed` is decremented by exactly `part->nbytes` (the sum of tuple bytes accounted during insertion), physically returning that memory to the allocator for reuse by the tuples that continue to arrive.

```c
/* ExecEHJFlushPartition — block reclamation loop */
EHJPartBlock block = part->head_block;
while (block != NULL)
{
    EHJPartBlock nextblock = block->next;
    pfree(block);          /* physical reclamation; no other partition affected */
    block = nextblock;
}
part->head_block = NULL;
part->tail_block = NULL;
```

Block allocation is handled by `EHJPartGetWritePtr` (a static helper in `nodeHash.c`). It writes into the tail block's data region and allocates a new block only when the current one is full. Block allocation does **not** touch `spaceUsed` — memory is accounted at tuple granularity, consistent with the standard `dense_alloc` policy.

### Block Size and Threshold

`EHJ_PART_BLOCK_SIZE` is 32 KB, matching `HASH_CHUNK_SIZE` from the standard allocator. The threshold for oversized tuples (`EHJ_PART_BLOCK_THRESHOLD = EHJ_PART_BLOCK_SIZE / 4`) mirrors `HASH_CHUNK_THRESHOLD`, giving oversized tuples their own dedicated block to avoid wasting capacity in normal-sized blocks.

---

## Architectural Decision: Double-Wide Bucket Array

The standard hash join allocates an array of `nbuckets` bucket-head pointers for the inner relation only. The outer relation is streamed and never stored.

EHJ must store both relations in memory simultaneously during Phase 1 and Phase 2. Rather than managing two separate arrays with separate size calculations, EHJ allocates a **single contiguous array of `2 * nbuckets` pointers** and splits it in half:

```
buckets.unshared[0 .. nbuckets-1]         ← inner (R) bucket heads
buckets.unshared[nbuckets .. 2*nbuckets-1] ← outer (S) bucket heads
```

A named alias `outer_buckets = &buckets.unshared[nbuckets]` is stored on the table so that probe loops can write `outer_buckets[bucketno]` without arithmetic at every call site. Both halves share the single `spaceUsed / spaceAllowed` budget, so the Phase 2 biased-flush policy can reason about the combined memory footprint without separate accounting.

The two-times overhead of the bucket pointer array is charged to `spaceUsed` immediately at table creation:

```c
hashtable->spaceUsed = 2 * nbuckets * sizeof(HashJoinTuple);
```

This ensures the memory-full check during Phase 1 insertion is accurate from the very first tuple.

---

## Architectural Decision: EHJ Tuple Layout (Arrival Timestamp)

Standard `HashJoinTupleData` stores a `next` pointer and a `hashvalue`, immediately followed by the `MinimalTuple` payload at offset `HJTUPLE_OVERHEAD`.

EHJ inserts an `int64 arrival_ts` field between the standard header and the `MinimalTuple`. This timestamp is the value of `ehj_current_tick` at the moment the tuple was read from its source, and is used by the Phase 3 duplicate-detection predicate `EHJShouldEmit` to determine whether a pair of tuples could have been matched in an earlier phase.

```
EHJ tuple memory layout (inside a partition block):

  offset 0                    : HashJoinTupleData  (next ptr + hashvalue)
  offset HJTUPLE_OVERHEAD     : int64 arrival_ts   (EHJ_TS_SIZE bytes, MAXALIGN'd)
  offset EHJ_HJTUPLE_OVERHEAD : MinimalTuple data  (t_len bytes)
```

All EHJ insertion and scan functions use `EHJ_HJTUPLE_MINTUPLE()` and `EHJ_HJTUPLE_ARRIVAL_TS()` instead of the standard `HJTUPLE_MINTUPLE()`. Using the wrong macro on an EHJ tuple would silently misinterpret the timestamp bytes as the start of the `MinimalTuple` header, producing corrupt results.

---

## Architectural Decision: Post-Freeze Write Buffer (`EHJBufferedTuple`)

Once a partition is flushed (frozen), its block chain no longer exists. A tuple that arrives late for a frozen partition cannot be inserted into memory. Two options exist:

1. Write each late-arriving tuple immediately to the partition's `BufFile` — one `BufFileWrite` syscall per tuple.
2. Buffer late-arriving tuples in memory and bulk-write them.

EHJ uses option 2. Each frozen partition maintains a small dynamically-sized array of `EHJBufferedTuple` entries (initial capacity `EHJ_PART_BUFFER_INITIAL = 64`). The `MinimalTuple` is copied into `hashCxt` (so it outlives per-tuple slot memory), and the entry records the `hashvalue` and `arrival_ts` needed for the Phase 3 on-disk format. When the buffer fills, `ExecEHJFlushPartitionBuffer` bulk-writes all entries to the `BufFile` in a single loop, `pfree`s each tuple copy, and decrements `spaceUsed` by `part->buffer_nbytes`. The buffer array itself is retained and reused for the next batch of post-freeze arrivals.

`ExecEHJFlushAllPartitionBuffers` is called at the end of Phase 2 to drain any remaining buffered tuples on both sides before Phase 3 begins, ensuring no tuples are silently dropped.

---

## Key Struct Changes

### `HashJoinState` (`src/include/nodes/execnodes.h`)

Six groups of new fields were added to `HashJoinState`:

**Reading policy counters (Phase 1 & 2)**
```c
int  read_ratio_inner;   /* A in the A:B interleaved-read strategy */
int  read_ratio_outer;   /* B in the A:B interleaved-read strategy */
int  reads_from_inner;   /* position counter within the current cycle */
int  reads_from_outer;   /* position counter within the current cycle */
```
These implement the configurable A:B reading ratio used during Phase 2 to control how many inner (R) tuples are read per outer (S) tuple before switching sides.

**Phase 2 pending-probe flags**
```c
bool  ehj_p2_pending_inner;  /* an R tuple is awaiting outer-bucket scan */
bool  ehj_p2_pending_outer;  /* an S tuple is awaiting inner-bucket scan */
```
After inserting a tuple in Phase 2, the state machine immediately scans the opposite bucket for matches. These flags preserve the pending probe state across state-machine re-entries.

**Phase 3 per-partition cleanup arrays**
```c
int           ehj_p3_partno;          /* current bucket being processed    */
MinimalTuple *ehj_p3_inner_tups;      /* loaded inner tuples for this part */
int64        *ehj_p3_inner_ts;        /* arrival timestamps                 */
uint32       *ehj_p3_inner_hv;        /* hash values                        */
int           ehj_p3_inner_count;     /* number of valid inner entries      */
MinimalTuple *ehj_p3_outer_tups;      /* loaded outer tuples for this part */
int64        *ehj_p3_outer_ts;
uint32       *ehj_p3_outer_hv;
int           ehj_p3_outer_count;
int           ehj_p3_ri;              /* inner-loop cursor in nested loop   */
int           ehj_p3_si;              /* outer-loop cursor in nested loop   */
```

### `HashJoinTableData` (`src/include/executor/hashjoin.h`)

**Double-wide bucket array.** For EHJ, `buckets` is allocated with `2 * nbuckets` entries. Buckets `[0, nbuckets)` hold inner (R) tuples; buckets `[nbuckets, 2*nbuckets)` hold outer (S) tuples. A named `outer_buckets` pointer is set to `&buckets.unshared[nbuckets]` for readability. Both sides share the single `spaceUsed` / `spaceAllowed` budget.

**Phase 1 state flags**
```c
bool  ehj_enabled;       /* true iff this table was created for EHJ      */
bool  ehj_phase1_done;   /* symmetric phase has ended                     */
bool  ehj_inner_done;    /* R iterator exhausted                          */
bool  ehj_outer_done;    /* S iterator exhausted                          */
```

**Phase 2 partition metadata**
```c
EHJPartData *ehj_inner_parts;   /* [nbuckets] inner-side partition state  */
EHJPartData *ehj_outer_parts;   /* [nbuckets] outer-side partition state  */
int64        ehj_current_tick;  /* monotonically increasing arrival clock */
```

### New Types (`src/include/executor/hashjoin.h`)

**`EHJPartData`** — per-partition metadata for one side (inner or outer) of one hash bucket, tracking whether the partition has been flushed to disk, its `BufFile`, its in-memory `EHJPartBlock` chain, and its post-freeze write buffer.

**`EHJPartBlockData` / `EHJPartBlock`** — a partition-exclusive memory block (32 KB). Each partition owns its own linked chain of blocks, enabling physical memory reclamation on flush via `pfree`. This is the critical departure from the standard `HashMemoryChunk` slab: blocks are never shared between partitions.

**`EHJBufferedTuple`** — a post-freeze tuple record (`hashvalue`, arrival timestamp, `MinimalTuple*`) held in a dynamically grown array until bulk-written to the partition's `BufFile`.

**EHJ tuple layout** — EHJ tuples carry an `int64 arrival_ts` field between the standard `HashJoinTupleData` header and the `MinimalTuple` payload. The macros `EHJ_HJTUPLE_OVERHEAD` and `EHJ_HJTUPLE_MINTUPLE()` replace the standard `HJTUPLE_OVERHEAD` / `HJTUPLE_MINTUPLE()` wherever EHJ tuples are used.

---

## EHJ Algorithm: Three-Phase State Machine

The join is executed by a state machine in `ExecHashJoinImpl` (`nodeHashjoin.c`) that drives all three phases.

### Phase 1 — Symmetric In-Memory Join (`HJ_EHJ_SYMMETRIC`)

Both the inner (R) and outer (S) relations are read **one tuple at a time**, alternating between sides. After each insertion the newly inserted tuple is immediately probed against the opposite bucket:

- An R tuple inserted → transitions to `HJ_EHJ_SCAN_OUTER_BUCKET` to probe the S bucket.
- An S tuple inserted → transitions to `HJ_EHJ_SCAN_INNER_BUCKET` to probe the R bucket.

Each scan state exhausts all matching tuples from the opposite bucket and emits join results before returning to `HJ_EHJ_SYMMETRIC`.

Phase 1 ends when either:
- **Memory fills** (`spaceUsed >= spaceAllowed`) — transitions to Phase 2.
- **Both sources are exhausted** — skips Phase 2 and transitions directly to Phase 3.

### Phase 2 — Biased Flush with Continued Reading (`HJ_EHJ_PHASE2_LOOP`)

Phase 2 continues reading from both sources using a configurable **A:B ratio** (`read_ratio_inner : read_ratio_outer`). Per cycle, A inner tuples are read then B outer tuples, with counters (`reads_from_inner`, `reads_from_outer`) tracking position.

When `spaceUsed >= spaceAllowed`, `ExecEHJBiasedFlush` is called before processing the next tuple. It loops until memory pressure is relieved, each iteration picking and flushing:
- The **largest non-frozen outer (S) partition**, or
- If none remain, the **smallest non-frozen inner (R) partition**.

This asymmetric victim-selection policy (largest S, smallest R) is intentional: it minimises the number of inner tuples written to disk, mirroring the standard join's preference for keeping the inner relation in memory.

For each newly inserted tuple, four cases arise based on which partitions are in memory or on disk:

| R partition | S partition | Action |
|---|---|---|
| In memory | In memory | Insert + probe opposite bucket (`PHASE2_SCAN_OUTER` or `PHASE2_SCAN_INNER`) |
| On disk   | In memory | Buffer R tuple; probe S bucket if S is in memory |
| In memory | On disk   | Insert R into memory; S will be joined in Phase 3 |
| On disk   | On disk   | Buffer the tuple; no immediate probe possible |

Phase 2 ends when both iterators are exhausted. `ExecEHJFlushAllPartitionBuffers` is called to drain all post-freeze buffers before transitioning to Phase 3.

### Phase 3 — Cleanup Join (`HJ_EHJ_PHASE3_NEXT_PART` / `HJ_EHJ_PHASE3_PROBE`)

Phase 3 iterates over all bucket indices `[0, nbuckets)`. For each bucket where at least one side has a disk file, it:

1. Loads all tuples from both `BufFile`s into palloc'd arrays (`ehj_p3_inner_tups`, `ehj_p3_outer_tups`) along with their arrival timestamps and hash values.
2. Performs a nested-loop join over the loaded arrays, filtered by the **`EHJShouldEmit` duplicate-detection predicate** (see below).
3. Frees the arrays and moves to the next partition.

This is implemented across two states: `HJ_EHJ_PHASE3_NEXT_PART` (load arrays, initialise loop cursors) and `HJ_EHJ_PHASE3_PROBE` (emit one result per state-machine tick, resumable across calls).

---

## Duplicate Detection: `EHJShouldEmit`

Because the same pair of tuples may have been eligible for a match in both Phase 1/2 (while both were in memory) and Phase 3 (after one was flushed to disk), Phase 3 must suppress duplicates. The `EHJShouldEmit` predicate accepts per-tuple arrival timestamps and per-partition flush timestamps and returns `true` only when a pair could **not** have been matched in an earlier phase:

```c
static bool
EHJShouldEmit(int64 tr_ts, int64 ts_ts,
              int64 tsf_r, int64 tsf_s,
              bool r_flushed, bool s_flushed)
{
    /* Case 1: S was in memory at flush time; R arrived after S was flushed */
    if (s_flushed && ts_ts <= tsf_s && tr_ts > tsf_s)
        return true;

    /* Case 2: S arrived after the S-partition flush but before the R flush,
               and R arrived after S — the pair is a new arrival post-freeze */
    if (s_flushed &&
        ts_ts > tsf_s &&
        (!r_flushed || ts_ts <= tsf_r) &&
        tr_ts > ts_ts)
        return true;

    /* Case 3: S arrived after the R-partition was flushed */
    if (r_flushed && ts_ts > tsf_r)
        return true;

    return false;
}
```

---

## New Functions (`src/include/executor/nodeHash.h`)

### Phase 1

| Function | Description |
|---|---|
| `ExecEHJHashTableCreate` | Creates a double-wide hash table (2 × nbuckets) and calls `ExecEHJInitPartitions`. |
| `ExecEHJTableInsertInner` | Inserts an R tuple into its partition's `EHJPartBlock` chain and updates accounting. Routes to `ExecEHJBufferFrozenTuple` if the partition is already frozen. |
| `ExecEHJTableInsertOuter` | Same as above for outer (S) tuples into `outer_buckets`. |
| `ExecEHJScanInnerBucket` | Iterates over the R half of a bucket; used to probe after an S insertion. Uses `EHJ_HJTUPLE_MINTUPLE()` to skip the arrival-timestamp field. |
| `ExecEHJScanOuterBucket` | Iterates over the S half of a bucket; used to probe after an R insertion. |

### Phase 2

| Function | Description |
|---|---|
| `ExecEHJInitPartitions` | Allocates and zero-initialises `ehj_inner_parts` and `ehj_outer_parts` arrays in `hashCxt`. |
| `ExecEHJPickVictimOuter` | Returns the bucket number of the largest non-frozen outer partition, or -1. |
| `ExecEHJPickVictimInner` | Returns the bucket number of the smallest non-frozen inner partition, or -1. |
| `ExecEHJFlushPartition` | Freezes a partition: writes tuples to disk, `pfree`s each `EHJPartBlock` in the chain (reclaiming memory), NULLs the bucket head, decrements `spaceUsed` by `part->nbytes`, initialises the post-freeze write buffer, and records `flush_ts`. |
| `ExecEHJBufferFrozenTuple` | Appends a new tuple for a frozen partition to its `EHJBufferedTuple` write buffer. Calls `ExecEHJFlushPartitionBuffer` automatically when full. |
| `ExecEHJFlushPartitionBuffer` | Bulk-writes a partition's write buffer to its `BufFile`, `pfree`s each `MinimalTuple` copy, decrements `spaceUsed` by `part->buffer_nbytes`, and resets the buffer. |
| `ExecEHJFlushAllPartitionBuffers` | Drains all non-empty post-freeze buffers on both sides. Must be called before Phase 3. |
| `ExecEHJBiasedFlush` | Top-level flush driver; loops until `spaceUsed < spaceAllowed` or all candidates are exhausted. |

---

## Files Modified

| File | Role |
|---|---|
| `src/include/nodes/execnodes.h` | Added EHJ fields to `HashJoinState` |
| `src/include/executor/hashjoin.h` | Added `EHJPartData`, `EHJPartBlock`, `EHJBufferedTuple`, double-wide bucket fields, EHJ tuple macros |
| `src/include/executor/nodeHash.h` | Declared all new EHJ functions |
| `src/backend/executor/nodeHashjoin.c` | Replaced HHJ state machine with EHJ three-phase state machine |
| `src/backend/executor/nodeHash.c` | Implemented all EHJ functions |
# EHJ Modification Notes

The original PostgreSQL hash join (Hybrid Hash Join) has been replaced in-place with an **Early Hash Join (EHJ)** as described in the [Early Hash Join paper](https://cmps-people.ok.ubc.ca/rlawrenc/research/Papers/EarlyHashJoin.pdf).

---

## Original vs. EHJ Architecture

The original Hybrid Hash Join built the entire inner relation into a hash table before reading from the outer relation. EHJ instead reads from both relations **symmetrically and simultaneously**, producing join results early and deferring spill decisions until memory pressure forces them.

---

## Plan and Execution State Trees

### Plan Tree Inheritance

**Node** → **Plan** → **Join** → **HashJoin**

### Execution State Tree Inheritance

**Node** → **PlanState** → **JoinState** → **HashJoinState**

---

## Key Struct Changes

### `HashJoinState` (`src/include/nodes/execnodes.h`)

Six groups of new fields were added to `HashJoinState`:

**Reading policy counters (Phase 1 & 2)**
```c
int  read_ratio_inner;   /* A in the A:B interleaved-read strategy */
int  read_ratio_outer;   /* B in the A:B interleaved-read strategy */
int  reads_from_inner;   /* position counter within the current cycle */
int  reads_from_outer;   /* position counter within the current cycle */
```
These implement the configurable A:B reading ratio used during Phase 2 to control how many inner (R) tuples are read per outer (S) tuple before switching sides.

**Phase 2 pending-probe flags**
```c
bool  ehj_p2_pending_inner;  /* an R tuple is awaiting outer-bucket scan */
bool  ehj_p2_pending_outer;  /* an S tuple is awaiting inner-bucket scan */
```
After inserting a tuple in Phase 2, the state machine immediately scans the opposite bucket for matches. These flags preserve the pending probe state across state-machine re-entries.

**Phase 3 per-partition cleanup arrays**
```c
int           ehj_p3_partno;          /* current bucket being processed    */
MinimalTuple *ehj_p3_inner_tups;      /* loaded inner tuples for this part */
int64        *ehj_p3_inner_ts;        /* arrival timestamps                 */
uint32       *ehj_p3_inner_hv;        /* hash values                        */
int           ehj_p3_inner_count;     /* number of valid inner entries      */
MinimalTuple *ehj_p3_outer_tups;      /* loaded outer tuples for this part */
int64        *ehj_p3_outer_ts;
uint32       *ehj_p3_outer_hv;
int           ehj_p3_outer_count;
int           ehj_p3_ri;              /* inner-loop cursor in nested loop   */
int           ehj_p3_si;              /* outer-loop cursor in nested loop   */
```

### `HashJoinTableData` (`src/include/executor/hashjoin.h`)

**Double-wide bucket array.** For EHJ, `buckets` is allocated with `2 * nbuckets` entries. Buckets `[0, nbuckets)` hold inner (R) tuples; buckets `[nbuckets, 2*nbuckets)` hold outer (S) tuples. A named `outer_buckets` pointer is set to `&buckets.unshared[nbuckets]` for readability. Both sides share the single `spaceUsed` / `spaceAllowed` budget.

**Phase 1 state flags**
```c
bool  ehj_enabled;       /* true iff this table was created for EHJ      */
bool  ehj_phase1_done;   /* symmetric phase has ended                     */
bool  ehj_inner_done;    /* R iterator exhausted                          */
bool  ehj_outer_done;    /* S iterator exhausted                          */
```

**Phase 2 partition metadata**
```c
EHJPartData *ehj_inner_parts;   /* [nbuckets] inner-side partition state  */
EHJPartData *ehj_outer_parts;   /* [nbuckets] outer-side partition state  */
int64        ehj_current_tick;  /* monotonically increasing arrival clock */
```

### New Types (`src/include/executor/hashjoin.h`)

**`EHJPartData`** — per-partition metadata for one side (inner or outer) of one hash bucket, tracking whether the partition has been flushed to disk, its BufFile, its in-memory block chain, and its post-freeze write buffer.

**`EHJPartBlockData` / `EHJPartBlock`** — a partition-exclusive memory block (32 KB, matching `HASH_CHUNK_SIZE`). Each partition owns its own linked chain of blocks, enabling physical memory reclamation on flush via `pfree`.

**`EHJBufferedTuple`** — a post-freeze tuple record (hashvalue, arrival timestamp, `MinimalTuple*`) held in a dynamically grown array until bulk-written to the partition's BufFile.

**EHJ tuple layout** — EHJ tuples carry an `int64 arrival_ts` field between the standard `HashJoinTupleData` header and the `MinimalTuple` payload. The macros `EHJ_HJTUPLE_OVERHEAD` and `EHJ_HJTUPLE_MINTUPLE()` replace the standard `HJTUPLE_OVERHEAD` / `HJTUPLE_MINTUPLE()` wherever EHJ tuples are used.

---

## EHJ Algorithm: Three-Phase State Machine

The join is executed by a state machine in `ExecHashJoinImpl` (`nodeHashjoin.c`) that drives all three phases.

### Phase 1 — Symmetric In-Memory Join (`HJ_EHJ_SYMMETRIC`)

Both the inner (R) and outer (S) relations are read **one tuple at a time**, alternating between sides. After each insertion the newly inserted tuple is immediately probed against the opposite bucket:

- An R tuple inserted → transitions to `HJ_EHJ_SCAN_OUTER_BUCKET` to probe the S bucket.
- An S tuple inserted → transitions to `HJ_EHJ_SCAN_INNER_BUCKET` to probe the R bucket.

Each scan state exhausts all matching tuples from the opposite bucket and emits join results before returning to `HJ_EHJ_SYMMETRIC`.

Phase 1 ends when either:
- **Memory fills** (`spaceUsed >= spaceAllowed`) — transitions to Phase 2.
- **Both sources are exhausted** — skips Phase 2 and transitions directly to Phase 3.

### Phase 2 — Biased Flush with Continued Reading (`HJ_EHJ_PHASE2_LOOP`)

Phase 2 continues reading from both sources using a configurable **A:B ratio** (`read_ratio_inner : read_ratio_outer`). Per cycle, A inner tuples are read then B outer tuples, with counters (`reads_from_inner`, `reads_from_outer`) tracking position.

When `spaceUsed >= spaceAllowed`, `ExecEHJBiasedFlush` is called before processing the next tuple. It loops until memory pressure is relieved, each iteration picking and flushing:
- The **largest non-frozen outer (S) partition**, or
- If none remain, the **smallest non-frozen inner (R) partition**.

For each newly inserted tuple, four cases arise based on which partitions are in memory or on disk:

| R partition | S partition | Action |
|---|---|---|
| In memory | In memory | Insert + probe opposite bucket (`PHASE2_SCAN_OUTER` or `PHASE2_SCAN_INNER`) |
| On disk   | In memory | Buffer R tuple; probe S bucket if S is in memory |
| In memory | On disk   | Insert R into memory; S will be joined in Phase 3 |
| On disk   | On disk   | Buffer the tuple; no immediate probe possible |

Phase 2 ends when both iterators are exhausted. `ExecEHJFlushAllPartitionBuffers` is called to drain all post-freeze buffers before transitioning to Phase 3.

### Phase 3 — Cleanup Join (`HJ_EHJ_PHASE3_NEXT_PART` / `HJ_EHJ_PHASE3_PROBE`)

Phase 3 iterates over all bucket indices `[0, nbuckets)`. For each bucket where at least one side has a disk file, it:

1. Loads all tuples from both BufFiles into palloc'd arrays (`ehj_p3_inner_tups`, `ehj_p3_outer_tups`) along with their arrival timestamps and hash values.
2. Performs a nested-loop join over the loaded arrays, filtered by the **`EHJShouldEmit` duplicate-detection predicate** (see below).
3. Frees the arrays and moves to the next partition.

This is implemented across two states: `HJ_EHJ_PHASE3_NEXT_PART` (load arrays, initialise loop cursors) and `HJ_EHJ_PHASE3_PROBE` (emit one result per state-machine tick, resumable across calls).

---

## Duplicate Detection: `EHJShouldEmit`

Because the same pair of tuples may have been eligible for a match in both Phase 1/2 (while both were in memory) and Phase 3 (after one was flushed to disk), Phase 3 must suppress duplicates. The `EHJShouldEmit` predicate accepts per-tuple arrival timestamps and per-partition flush timestamps and returns `true` only when a pair could **not** have been matched in an earlier phase:

```c
static bool
EHJShouldEmit(int64 tr_ts, int64 ts_ts,
              int64 tsf_r, int64 tsf_s,
              bool r_flushed, bool s_flushed)
{
    /* Case 1: S was in memory at flush time; R arrived after S was flushed */
    if (s_flushed && ts_ts <= tsf_s && tr_ts > tsf_s)
        return true;

    /* Case 2: S arrived after the S-partition flush but before the R flush,
               and R arrived after S — the pair is a new arrival post-freeze */
    if (s_flushed &&
        ts_ts > tsf_s &&
        (!r_flushed || ts_ts <= tsf_r) &&
        tr_ts > ts_ts)
        return true;

    /* Case 3: S arrived after the R-partition was flushed */
    if (r_flushed && ts_ts > tsf_r)
        return true;

    return false;
}
```

---

## New Functions (`src/include/executor/nodeHash.h`)

### Phase 1

| Function | Description |
|---|---|
| `ExecEHJHashTableCreate` | Creates a double-wide hash table (2 × nbuckets) and calls `ExecEHJInitPartitions`. |
| `ExecEHJTableInsertInner` | Inserts an R tuple into its partition block and updates accounting. |
| `ExecEHJTableInsertOuter` | Inserts an S tuple into its partition block and updates accounting. |
| `ExecEHJScanInnerBucket` | Iterates over the R half of a bucket; used to probe after an S insertion. |
| `ExecEHJScanOuterBucket` | Iterates over the S half of a bucket; used to probe after an R insertion. |

### Phase 2

| Function | Description |
|---|---|
| `ExecEHJInitPartitions` | Allocates and zero-initialises `ehj_inner_parts` and `ehj_outer_parts` arrays in `hashCxt`. |
| `ExecEHJPickVictimOuter` | Returns the bucket number of the largest non-frozen outer partition, or -1. |
| `ExecEHJPickVictimInner` | Returns the bucket number of the smallest non-frozen inner partition, or -1. |
| `ExecEHJFlushPartition` | Freezes a partition: writes tuples to disk, pfrees all blocks, initialises post-freeze write buffer. |
| `ExecEHJBufferFrozenTuple` | Appends a new tuple for a frozen partition to its in-memory write buffer. |
| `ExecEHJFlushPartitionBuffer` | Bulk-writes a partition's write buffer to its BufFile and resets the buffer. |
| `ExecEHJFlushAllPartitionBuffers` | Drains all non-empty post-freeze buffers on both sides. |
| `ExecEHJBiasedFlush` | Top-level flush driver; loops until `spaceUsed < spaceAllowed` or all candidates are exhausted. |

---

## Files Modified

| File | Role |
|---|---|
| `src/include/nodes/execnodes.h` | Added EHJ fields to `HashJoinState` |
| `src/include/executor/hashjoin.h` | Added `EHJPartData`, `EHJPartBlock`, `EHJBufferedTuple`, double-wide bucket fields, EHJ tuple macros |
| `src/include/executor/nodeHash.h` | Declared all new EHJ functions |
| `src/backend/executor/nodeHashjoin.c` | Replaced HHJ state machine with EHJ three-phase state machine |
| `src/backend/executor/nodeHash.c` | Implemented all EHJ functions |