#!/usr/bin/env python3
"""
ROSL estimator accuracy + efficiency worker (one job).

Handles one (size, q_name, z_val, shuffle) combination for all REPEATS.
Stdout/stderr are captured by rosl_estimator_manager.py into a .nohup.log file.

Tracks two things per round, not just one:
  * accuracy   -- est_join vs the cached truth, as ratio / rel_error (unchanged).
  * efficiency -- wall-clock cost, via elapsed_sec / delta_sec per round.

The tricky part is that phase 1 can't return even its first row until it has
*finished* -- so every round's NOTICE arrives during a single blocking
fetchone() call, with no row-by-row fetch loop to time from the call site
(that's how tpch_manager.py/worker.py do it, but it doesn't apply here). To
recover real per-round timing anyway, conn.notices is swapped for a deque
subclass (_TimestampedNotices, below) that stamps the wall-clock instant each
NOTICE is appended -- which happens as it's parsed off the wire, *during* the
blocking call, not after it returns.

summary.csv also gets a per-repeat wall time (repeat_wall_sec) and the
one-time cost of computing truth (truth_compute_sec, 0.0 on a cache hit), and
the manager now writes a job_timing.csv with one wall-clock row per
(size, z, shuffle, query) job -- so efficiency can be compared at three
granularities: per round, per repeat, and per job.

Truth counts are cached in {results_dir}/truth_counts.json, protected by an
advisory .lock sidecar file so concurrent workers never double-compute the same
cardinality.  The compute step is intentionally done outside the lock (it's the
expensive part); only the cache read and write are locked.

  >>> SCALE FACTOR <<<
  The number of ROSL rounds grows like (|R|/M_LIM)*(|S|/K_LIM) with M_LIM=K_LIM=1588.
  On 0.01g that is a few hundred rounds; on 1g millions; on 10g hundreds of
  millions -- infeasible to emit or capture.  Use 01 or 1 scale only.

Usage (direct):
    python3 rosl_estimator_worker.py <size> <q_name> <z_val> <shuffle> <results_dir> <limit>

    <limit>  pass "1" to apply LIMIT constraints, "0" for full output.
             The manager passes this automatically via --limit.
"""
import os
import re
import sys
import json
import time
import fcntl
import collections
import datetime
import psycopg2

# ── connection: the ALT cluster ──────────────────────────────────────────────
USER = "jinjo"
HOST = "/tmp/"      # socket dir; alt's socket lives here on its own port
PORT = "1533"       # alt cluster port (baseline is 1532)

# ── knobs ────────────────────────────────────────────────────────────────────
REPEATS         = 3           # re-runs per layout (RNG variance); reuse 1 conn so the RNG advances
WORK_MEM        = "64kB"      # matches the OSL timing convention; not critical for ROSL
TRUTH_TIMEOUT_S = 0           # 0 = no timeout for the one-time full-join count
ROSL_TIMEOUT_S  = 1800        # 30 min guard for a single phase-1 pass
NOTICE_CAP      = 1_000_000   # trajectory buffer; warns if it fills (scale too big)

# 2R inner-equijoins only -- the fixed-inner shape the estimator assumes.
# (q_name: (tableA, tableB, join_predicate))
QUERIES = {
    "Q9":  ("partsupp", "lineitem", "ps_partkey = l_partkey"),
    "Q10": ("customer", "orders",   "c_custkey = o_custkey"),
    "Q11": ("orders",   "lineitem", "o_orderdate = l_shipdate"),
    "Q12": ("orders",   "lineitem", "o_orderkey = l_orderkey"),
    "Q15": ("supplier", "lineitem", "s_suppkey = l_suppkey"),
}

# ── per-query output limits (edit here to adjust; must stay in sync with manager) ──
# Limits from tpch_manager.py as of 5/27/2026.
QUERY_LIMITS = {
    "Q9":  221700,
    "Q10": 13000,
    "Q11": 327624700,
    "Q12": 1000,
    "Q15": 43800,
}

def get_queries(q_name, schema, apply_limit):
    """Return join_sql for one (query, schema, limit) combination.
    Mirrors tpch_manager.py's get_queries() so output limits are easy to
    find and adjust in one place.  Edit QUERY_LIMITS above to change caps."""
    if q_name not in QUERIES:
        return None
    tA, tB, pred = QUERIES[q_name]
    base = f"SELECT * FROM {schema}.{tA}, {schema}.{tB} WHERE {pred}"
    if apply_limit and q_name in QUERY_LIMITS:
        return f"{base} LIMIT {QUERY_LIMITS[q_name]};"
    return f"{base};"


RE_ROUND = re.compile(
    r"ROSL estimate: mean_per_pair=([\d.eE+-]+) est_join=([\d.eE+-]+) "
    r"\(round=(\d+) pairs_seen=([\d.eE+-]+) sample_matches=(\d+)\)"
)

# Fired by PH_NEW_MBLOCK the instant the outer relation is exhausted.
# This is the authoritative signal that ROSL has finished; we stop fetching
# as soon as we see it rather than relying solely on ROSL_TIMEOUT_S.
RE_DONE = re.compile(r"ROSL done: sampling join complete")

# ── timing trick: timestamp NOTICEs the instant they arrive ──────────────────

class _TimestampedNotices(collections.deque):
    """Drop-in replacement for conn.notices that records a wall-clock
    timestamp the instant each NOTICE is appended.

    psycopg2's C extension calls .append() on whatever object is assigned to
    conn.notices directly from its libpq notice-processor callback, and that
    callback fires as each NOTICE is parsed off the wire -- which happens
    *during* a blocking call like cursor.fetchone(), not after it returns.
    Overriding append() lets us steal a time.time() reading at that exact
    moment, which is the only way to get real per-round timing out of
    run_estimate()'s single all-the-rounds-in-one-fetch call below.
    """
    def __init__(self, maxlen=None):
        super().__init__(maxlen=maxlen)
        self.timestamps = collections.deque(maxlen=maxlen)

    def append(self, item):
        self.timestamps.append(time.time())
        super().append(item)

# ── truth cache (file-locked for concurrent workers) ──────────────────────────

def _load_cache(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_cache(cache, path):
    # Atomic: write a temp file then rename over the target.
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(cache, fh, indent=2, sort_keys=True)
    os.replace(tmp, path)

def get_truth(conn, cache_path, key, schema, tA, tB, pred):
    """Return (exact join cardinality, compute_sec), computing the count once
    and caching it.  compute_sec is 0.0 on a cache hit -- this run paid no
    truth-finding cost -- or the measured seconds when this run had to
    compute it, so summary.csv can show that one-time cost separately from
    the per-round estimator timing.

    Pattern: shared read lock (fast path) -> compute without lock (expensive) ->
    exclusive write lock with double-check (prevents duplicate writes from a race).
    """
    lock_path = cache_path + ".lock"

    # Fast path: acquire a shared read lock and check the cache.
    with open(lock_path, 'a') as lf:
        fcntl.flock(lf, fcntl.LOCK_SH)
        try:
            cache = _load_cache(cache_path)
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)
    if key in cache:
        print(f"  [truth] {key} = {cache[key]:,} (cached)", flush=True)
        return cache[key], 0.0

    # Compute without holding the lock (full join count can be very slow).
    print(f"  [truth] computing {key} (forced hash join, one-time) ...", flush=True)
    t0 = time.time()
    cur = conn.cursor()
    for stmt in (
        "SET enable_rosl = off;",
        "SET enable_nestloop = off;",
        "SET enable_mergejoin = off;",
        "SET enable_hashjoin = on;",
        "SET enable_material = on;",
        "SET enable_seqscan = on;",
        "SET enable_indexscan = on;",
        "SET enable_indexonlyscan = on;",
        "SET enable_bitmapscan = on;",
        "SET enable_fastjoin = off;",
        "SET enable_block = off;",
        "SET enable_fliporder = off;",
        f"SET statement_timeout = {TRUTH_TIMEOUT_S * 1000};",
    ):
        cur.execute(stmt)
    cur.execute(f"SELECT count(*) FROM {schema}.{tA}, {schema}.{tB} WHERE {pred};")
    truth = int(cur.fetchone()[0])
    cur.close()
    conn.commit()
    compute_sec = time.time() - t0
    print(f"  [truth] {key} = {truth:,}  ({compute_sec:.1f}s)", flush=True)

    # Exclusive write lock with double-check: another worker may have beaten us.
    with open(lock_path, 'a') as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            cache = _load_cache(cache_path)
            if key not in cache:
                cache[key] = truth
                _save_cache(cache, cache_path)   # persist immediately -> never recompute
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)

    return truth, compute_sec

# ── ROSL estimate (one phase-1 pass) ─────────────────────────────────────────

def configure_rosl(conn):
    """Force a plain, fixed-inner nested loop and turn ROSL on (session-level)."""
    cur = conn.cursor()
    for stmt in (
        "SET enable_material = off;",            # no Materialize between NL and inner
        "SET max_parallel_workers_per_gather = 0;",
        "SET enable_hashjoin = off;",
        "SET enable_mergejoin = off;",
        "SET enable_indexonlyscan = off;",
        "SET enable_indexscan = off;",           # inner cannot be a param'd index scan -> fixed inner
        "SET enable_block = off;",
        "SET enable_bitmapscan = off;",
        "SET enable_fastjoin = off;",
        "SET enable_seqscan = off;",             # soft-penalised; still used (no alternative)
        "SET enable_fliporder = off;",
        "SET enable_nestloop = on;",
        "SET enable_rosl = on;",                 # <-- the gate we added
        f"SET work_mem = '{WORK_MEM}';",
        f"SET statement_timeout = {ROSL_TIMEOUT_S * 1000};",
    ):
        cur.execute(stmt)
    cur.close()
    conn.commit()

def run_estimate(conn, join_sql):
    """Run the join under ROSL and pull rows until the engine says it is done,
    returning (rounds, call_wall_sec, n_rows).

    WHY THE OLD single-fetchone() NO LONGER WORKS
    -----------------------------------------------
    The new nodeNestloop.c is streaming: PH_PROBE now calls ExecProject() and
    returns a real tuple for every sample match, one per ExecNestLoop() call.
    A single fetchone() therefore returns only the *first* matched row, not
    "all of phase 1".  All subsequent per-round NOTICEs -- and crucially the
    "ROSL done" NOTICE emitted in PH_NEW_MBLOCK when the outer relation is
    exhausted -- only arrive during later fetches that the old code never made.
    Consequences of stopping after one fetch:

      1. Only round 1's NOTICE is seen; pct_of_final_pairs normalises every
         repeat against that single tiny slice (0→100% means nothing across
         repeats or configs).
      2. The "ROSL done" NOTICE is never observed, so we can't detect
         completion early and must rely on ROSL_TIMEOUT_S as the only exit.
      3. conn.rollback() closes the cursor mid-stream, leaving ROSL in an
         arbitrary internal state -- harmless for accuracy but wastes the
         work already done.

    THE FIX
    -------
    Loop fetchone() until it returns None *or* we see RE_DONE in conn.notices.
    The "done" NOTICE and the final None are effectively simultaneous (PH_DONE
    sets st->phase then returns NULL in the same ExecNestLoop call as the
    NOTICE fires), so checking both is belt-and-suspenders, not two distinct
    events.  n_rows counts actual sample-match rows pulled through (their
    contents are never inspected -- only the NOTICE stream matters for accuracy
    and timing).

    pct_of_final_pairs is still normalised against this repeat's own last
    round's pairs_seen (rounds[-1]["pairs_seen"]), preserving the 0→100%
    semantics -- but now "last round" means ROSL's actual last round, not
    just whatever happened before the first row came back.
    """
    conn.notices = _TimestampedNotices(maxlen=NOTICE_CAP)

    sc = conn.cursor(name="rosl_cur")    # server-side cursor; itersize=1 keeps
    sc.itersize = 1                      # each fetch a single round-trip
    sc.execute(join_sql)
    t_start = time.time()
    n_rows = 0

    while True:
        row = sc.fetchone()

        # Check for the "done" NOTICE before testing row==None: the NOTICE
        # is appended to conn.notices *during* the fetchone() that returns
        # None, so by the time we arrive here it is already present.
        done_seen = any(RE_DONE.search(msg) for msg in conn.notices)

        if row is None or done_seen:
            break
        n_rows += 1

        if len(conn.notices) >= NOTICE_CAP:
            # Buffer full mid-stream; stop now rather than losing more data.
            print("  WARNING: notice buffer full -- trajectory TRUNCATED mid-run. "
                  "Use a smaller scale factor.", flush=True)
            break

    call_wall_sec = time.time() - t_start
    notices    = list(conn.notices)
    arrival_ts = list(conn.notices.timestamps)
    sc.close()
    conn.rollback()

    if not done_seen:
        print("  WARNING: ROSL done notice never seen -- run may have been "
              "cut off by statement_timeout or notice-buffer overflow.", flush=True)

    rounds = []
    prev_ts = t_start
    for n, ts in zip(notices, arrival_ts):
        m = RE_ROUND.search(n)
        if m:
            rounds.append({
                "round":          int(m.group(3)),
                "mean_per_pair":  float(m.group(1)),
                "est_join":       float(m.group(2)),
                "pairs_seen":     float(m.group(4)),
                "sample_matches": int(m.group(5)),
                "elapsed_sec":    ts - t_start,
                "delta_sec":      ts - prev_ts,
            })
            prev_ts = ts
    rounds.sort(key=lambda r: r["round"])
    return rounds, call_wall_sec, n_rows

# ── entry point ───────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) != 7:
        print("Usage: python3 rosl_estimator_worker.py "
              "<size> <q_name> <z_val> <shuffle> <results_dir> <limit>")
        sys.exit(1)

    size, q_name, z_val, shuffle, results_dir, limit_flag = sys.argv[1:]
    apply_limit = limit_flag == "1"

    if q_name not in QUERIES:
        print(f"Unknown query '{q_name}'. Valid: {', '.join(QUERIES)}", flush=True)
        sys.exit(1)

    os.makedirs(results_dir, exist_ok=True)

    cache_path = os.path.join(results_dir, "truth_counts.json")
    prefix     = os.path.join(results_dir, f"{q_name}_{size}_z{z_val}_sch{shuffle}")
    traj_path  = f"{prefix}.traj.csv"
    summ_path  = f"{prefix}.summ.csv"

    tA, tB, pred = QUERIES[q_name]
    schema   = f"z{z_val}_shuff{shuffle}"
    key      = f"{size}|z{z_val}|sch{shuffle}|{q_name}"
    join_sql = get_queries(q_name, schema, apply_limit)
    db       = f"tpch{size}g"

    print(f"=== {q_name}  size={size}  z={z_val}  sch={shuffle}  db={db}  "
          f"limit={'ON' if apply_limit else 'OFF'} ===", flush=True)

    try:
        conn = psycopg2.connect(dbname=db, user=USER, host=HOST, port=PORT)
    except Exception as e:
        print(f"  cannot connect to {db}: {e}", flush=True)
        sys.exit(1)

    try:
        truth, truth_compute_sec = get_truth(conn, cache_path, key, schema, tA, tB, pred)
    except Exception as e:
        print(f"  [truth] FAILED: {e}", flush=True)
        conn.rollback()
        conn.close()
        sys.exit(1)

    with open(traj_path, "w") as traj_f, open(summ_path, "w") as summ_f:
        traj_f.write("size,query,zval,shuffle,repeat,round,pairs_seen,sample_matches,"
                     "mean_per_pair,est_join,truth,ratio,rel_error,"
                     "elapsed_sec,delta_sec,pct_of_truth_output\n")
        # n_rows column added: actual sample-match rows pulled through the
        # cursor (contents ignored; count is a sanity-check on streaming output).
        summ_f.write("size,query,zval,shuffle,repeat,truth,final_est,final_ratio,n_rounds,"
                     "n_rows,repeat_wall_sec,truth_compute_sec\n")

        configure_rosl(conn)

        for rep in range(1, REPEATS + 1):
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"  [{ts}] estimate {key} repeat {rep}/{REPEATS}", flush=True)
            try:
                rounds, call_wall_sec, n_rows = run_estimate(conn, join_sql)
            except Exception as e:
                print(f"  [estimate] FAILED rep {rep}: {e}", flush=True)
                conn.rollback()
                continue

            for r in rounds:
                ratio = (r["est_join"] / truth) if truth else float("nan")
                rerr  = ((r["est_join"] - truth) / truth) if truth else float("nan")
                # pct_of_truth_output: fraction of ground-truth join rows that
                # ROSL's sampling has produced so far.  sample_matches is the
                # cumulative count of actual join-result rows emitted by the
                # engine; truth is the exact join cardinality from the hash join
                # or JSON cache.  This gives a meaningful x-axis (how much of
                # the real output have we seen?) rather than pairs_seen/truth,
                # which mixed two unrelated quantities (pair coverage vs. output
                # cardinality), or pairs_seen/final_pairs, which was only
                # self-comparable within a single repeat.
                pct   = (r["sample_matches"] / truth * 100.0) if truth else float("nan")
                traj_f.write("%s,%s,%s,%s,%d,%d,%.0f,%d,%.10f,%.2f,%d,%.6f,%.6f,%.4f,%.4f,%.4f\n" % (
                    size, q_name, z_val, shuffle, rep, r["round"], r["pairs_seen"],
                    r["sample_matches"], r["mean_per_pair"], r["est_join"],
                    truth, ratio, rerr, r["elapsed_sec"], r["delta_sec"], pct))

            if rounds:
                fe = rounds[-1]["est_join"]
                fr = (fe / truth) if truth else float("nan")
                summ_f.write("%s,%s,%s,%s,%d,%d,%.2f,%.6f,%d,%d,%.3f,%.3f\n" % (
                    size, q_name, z_val, shuffle, rep, truth, fe, fr, len(rounds),
                    n_rows, call_wall_sec, truth_compute_sec))
                print(f"      final est={fe:,.0f}  truth={truth:,}  ratio={fr:.4f}  "
                      f"rounds={len(rounds)}  n_rows={n_rows}  wall={call_wall_sec:.1f}s",
                      flush=True)
            else:
                print(f"      no ROSL notices parsed (wall={call_wall_sec:.1f}s) -- is "
                      "enable_rosl wired and the plan a plain nested loop?", flush=True)

            traj_f.flush()
            summ_f.flush()

    conn.close()
    print(f"Done. -> {prefix}.[traj|summ].csv", flush=True)

if __name__ == "__main__":
    main()