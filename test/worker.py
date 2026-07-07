#!/usr/bin/env python3
"""
ROSL estimator accuracy + efficiency worker (one job).

Handles one (size, q_name, z_val, shuffle) combination for all REPEATS.
Stdout/stderr are captured by tpch_manager.py into a .nohup.log file.

Tracks two things per round, not just one:
  * accuracy   -- est_join vs the cached truth, as ratio / rel_error (unchanged).
  * efficiency -- wall-clock cost, via elapsed_sec / delta_sec per round.

COMMUNICATION MODEL (Saketh-compatible, log-only)
-------------------------------------------------
The ROSL C node no longer emits per-round NOTICEs mid-stream.  Instead it
accumulates the whole per-round trajectory inside its executor state and dumps
it ONCE, at executor teardown, as elog(INFO, ...) lines to the server log:

    ROSL_TRAJ round=.. mean_per_pair=.. est_join=.. ci_halfwidth=.. \
              pairs_seen=.. sample_matches=.. elapsed_ms=..
    ROSL_SUMM final_est_join=.. ci_halfwidth=.. rounds=.. sample_matches=.. ...

This worker therefore does NOT read conn.notices at all.  Its only job during
the join is to drain rows (so the executor actually runs to teardown) and then,
after the cursor closes, to read its own statement's ROSL_TRAJ/ROSL_SUMM lines
out of the PostgreSQL server log.  Because measurement delivery is fully
decoupled from row delivery, a server-side cursor can never deadlock on notice
flushing -- the bug that motivated this rewrite is structurally gone.

Per-job log attribution under concurrency:
  * the active log path is resolved per job via pg_current_logfile() (it can
    rotate mid-sweep, so we never cache it);
  * the worker records the log's byte size immediately before the run and reads
    only bytes appended after that point;
  * it learns its own backend PID via pg_backend_pid() and keeps only lines
    whose "[<pid>]" prefix matches (log_line_prefix is "%m [%p] ");
  * it brackets each run with a unique sentinel (RAISE INFO 'ROSL_MARK <uuid>')
    emitted on the same backend, so even if PID prefixes were absent the run's
    own lines can be isolated between its two markers.
Per-round wall-clock timing now comes from the C-measured elapsed_ms column,
which is taken at the source and is independent of when the client fetched.

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
    python3 worker.py <size> <q_name> <z_val> <shuffle> <results_dir> <limit>

    <limit>  pass "1" to apply LIMIT constraints, "0" for full output.
             The manager passes this automatically via --limit.
"""
import os
import re
import sys
import json
import time
import uuid
import fcntl
import datetime
import psycopg2

# ── connection: the ALT cluster ──────────────────────────────────────────────
USER = "jinjo"
HOST = "/tmp/"      # socket dir; alt's socket lives here on its own port
PORT = "1533"       # alt cluster port (baseline is 1532)

# ── knobs ────────────────────────────────────────────────────────────────────
REPEATS         = 1           # re-runs per layout (RNG variance); reuse 1 conn so the RNG advances
WORK_MEM        = "64kB"      # matches the OSL timing convention; not critical for ROSL
TRUTH_TIMEOUT_S = 0           # 0 = no timeout for the one-time full-join count
ROSL_TIMEOUT_S  = 1800        # 30 min guard for a single phase-1 pass
ITERSIZE        = 2000        # server-side cursor batch; >1 avoids a round-trip per row
DRAIN_TIMEOUT_S = 1800        # client-side wall-clock guard on the row-drain loop
LOG_READ_WAIT_S = 10.0        # grace period for the teardown dump to flush to the log
LOG_READ_EXTRA_S = 20.0       # extra wait once we've seen the open sentinel but not the close
LOG_POLL_S      = 0.2         # poll interval while waiting for the closing sentinel

# ── query shapes and output caps: imported from tpch_manager ─────────────────
# tpch_manager.py is the single source of truth for QUERY_DEFS / QUERY_LIMITS;
# the previous hand-synced mirror here is how caps drift between manager and
# worker.  A frozen fallback keeps the worker runnable standalone, but drift is
# then possible again -- hence the loud warning.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from tpch_manager import QUERY_DEFS, QUERY_LIMITS
except ImportError:
    print("WARNING: could not import tpch_manager.py -- using frozen fallback "
          "query definitions (caps may drift from the manager's).", flush=True)
    QUERY_DEFS = {
        "Q9":  (("partsupp", "lineitem"), "ps_partkey = l_partkey"),
        "Q10": (("customer", "orders"),   "c_custkey = o_custkey"),
        "Q11": (("orders",   "lineitem"), "o_orderdate = l_shipdate"),
        "Q12": (("orders",   "lineitem"), "o_orderkey = l_orderkey"),
        "Q15": (("supplier", "lineitem"), "s_suppkey = l_suppkey"),
    }
    QUERY_LIMITS = {
        "Q9":  221700,
        "Q10": 13000,
        "Q11": 327624700,
        "Q12": 1000,
        "Q15": 43800,
    }

# 2R inner-equijoins only -- the fixed-inner shape the estimator assumes.
# (q_name: (tableA, tableB, join_predicate)); 3R entries in QUERY_DEFS are
# filtered out here, and main() rejects them with a clear message.
QUERIES = {q: (tabs[0], tabs[1], pred)
           for q, (tabs, pred) in QUERY_DEFS.items() if len(tabs) == 2}

def get_queries(q_name, schema, apply_limit):
    """Return join_sql for one (query, schema, limit) combination.  Caps come
    from tpch_manager.QUERY_LIMITS -- edit them there, nowhere else."""
    if q_name not in QUERIES:
        return None
    tA, tB, pred = QUERIES[q_name]
    base = f"SELECT * FROM {schema}.{tA}, {schema}.{tB} WHERE {pred}"
    if apply_limit and q_name in QUERY_LIMITS:
        return f"{base} LIMIT {QUERY_LIMITS[q_name]};"
    return f"{base};"


# ── server-log line parsers (the new measurement channel) ────────────────────
# These match the elog(INFO, ...) lines PrintRoslCounters() writes at teardown.
# A log line looks like:  2026-06-23 00:35:13.123 PDT [12345] INFO:  ROSL_TRAJ ...
#
# The trailing ci_eb group is optional so logs from an older engine build
# (before the per-round EB interval was added) still parse.
RE_TRAJ = re.compile(
    r"ROSL_TRAJ round=(\d+) mean_per_pair=([\d.eE+-]+) est_join=([\d.eE+-]+) "
    r"ci_halfwidth=([\d.eE+-]+) pairs_seen=([\d.eE+-]+) sample_matches=(\d+) "
    r"elapsed_ms=([\d.eE+-]+)(?: ci_eb=([\d.eE+-]+))?"
)
# ROSL_SUMM is parsed as key=value tokens, NOT positionally.  The old
# positional regex required "rounds=" to follow "ci_halfwidth=" immediately,
# but the engine emits ci_clust/ci_split/ci_eb/est_eb/ci_noise/het_ratio/
# lindeberg_max/n_blocks in between -- so it NEVER matched, and every run
# silently fell back to the last trajectory round's est/CI instead of the
# teardown values recomputed against the exact act_outer.
RE_SUMM_LINE = re.compile(r"ROSL_SUMM\s+(.*)")
RE_KV = re.compile(r"(\w+)=([\d.eE+-]+|nan|inf|-inf)")
RE_TRAJ_TRUNC = re.compile(r"ROSL_TRAJ truncated")
# Per-line backend-PID prefix, from log_line_prefix = '%m [%p] '.
RE_PID = re.compile(r"\[(\d+)\]")

# ── server-log reading (the new measurement channel) ─────────────────────────

def _resolve_logfile(conn):
    """Return the absolute path of the cluster's active stderr log, or None.

    Resolved per call (never cached) because the collector rotates by size/age
    and a long sweep can roll over mid-run.  pg_current_logfile() returns a
    path relative to data_directory when logging_collector is on.
    """
    cur = conn.cursor()
    cur.execute("SELECT pg_current_logfile()")
    row = cur.fetchone()
    rel = row[0] if row else None
    if not rel:
        cur.close()
        return None
    if os.path.isabs(rel):
        cur.close()
        return rel
    cur.execute("SHOW data_directory")
    data_dir = cur.fetchone()[0]
    cur.close()
    return os.path.join(data_dir, rel)


def _logfile_size(path):
    """Current size of the log in bytes, or 0 if it can't be stat'd."""
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _read_log_tail(path, start_offset):
    """Read text appended to `path` since byte offset `start_offset`.

    Tolerant of rotation: if the file is now shorter than start_offset (a new
    file took its name, or it was truncated), read from the beginning instead.
    """
    try:
        size = os.path.getsize(path)
        with open(path, "r", errors="replace") as fh:
            fh.seek(0 if size < start_offset else start_offset)
            return fh.read()
    except OSError:
        return ""


def _read_log_since(start_path, start_offset, start_mtime):
    """Read this run's log text across a possible rotation.

    A long-draining job (big scale, Q9/Q11) can run long enough that the
    logging collector rotates the file (default log_rotation_size=10MB) WHILE we
    drain.  The teardown ROSL_TRAJ/ROSL_SUMM dump and our closing sentinel then
    land in a NEWER file than start_path, which the single-path reader would
    never see -- the cause of "no ROSL_TRAJ lines found" on otherwise-valid long
    runs.

    To be robust we read:
      1. the tail of start_path from start_offset (lines before any rotation), and
      2. the FULL contents of every other log file in the same directory whose
         mtime is >= start_mtime (i.e. any file the run could have rotated into).
    The text is concatenated in filename order (collector names are timestamped,
    so lexical order is chronological).  Correctness does not depend on getting
    the file boundaries exactly right: the unique open/close sentinels bound our
    run's lines wherever they landed, and _parse_rosl_log filters on them plus
    our backend PID.  Reading a little extra is harmless.
    """
    parts = []
    log_dir = os.path.dirname(start_path)
    start_base = os.path.basename(start_path)

    # (1) tail of the original file
    parts.append(_read_log_tail(start_path, start_offset))

    # (2) any sibling log files created/modified at or after we started
    try:
        entries = []
        for name in os.listdir(log_dir):
            if name == start_base:
                continue
            full = os.path.join(log_dir, name)
            try:
                st = os.stat(full)
            except OSError:
                continue
            # Only plausible rotation targets: regular files touched since start.
            if not os.path.isfile(full):
                continue
            if st.st_mtime + 1.0 < start_mtime:   # 1s slack for fs mtime coarseness
                continue
            entries.append((name, full))
        # timestamped collector filenames sort chronologically by name
        for _name, full in sorted(entries):
            try:
                with open(full, "r", errors="replace") as fh:
                    parts.append(fh.read())
            except OSError:
                continue
    except OSError:
        pass

    return "\n".join(p for p in parts if p)


def _parse_rosl_log(text, pid, open_mark, close_mark):
    """Extract this run's ROSL_TRAJ rows and ROSL_SUMM from raw log `text`.

    Two filters isolate *our* statement's lines from a log many concurrent
    workers share:
      1. line must carry our backend PID in its "[<pid>]" prefix, AND
      2. line must fall between our unique open/close sentinels.
    Either filter alone would usually suffice; together they are robust to PID
    reuse across the sweep and to interleaving from other backends.

    Returns (rounds, final_summary_or_None, truncated_bool).
    """
    rounds = []
    final = None
    truncated = False

    in_window = False
    pid_tag = f"[{pid}]"

    for line in text.splitlines():
        # Gate on our sentinels first so we never read another run's window.
        if open_mark in line:
            in_window = True
            continue
        if close_mark in line:
            in_window = False
            continue
        if not in_window:
            continue
        # Within our window, still require our PID to be safe under interleave.
        if pid_tag not in line:
            continue

        mt = RE_TRAJ.search(line)
        if mt:
            rounds.append({
                "round":          int(mt.group(1)),
                "mean_per_pair":  float(mt.group(2)),
                "est_join":       float(mt.group(3)),
                "ci_halfwidth":   float(mt.group(4)),
                "pairs_seen":     float(mt.group(5)),
                "sample_matches": int(mt.group(6)),
                "elapsed_sec":    float(mt.group(7)) / 1000.0,  # ms -> sec
                # Per-round anytime-valid EB interval; nan on older engine
                # builds whose ROSL_TRAJ lines lack the field.
                "ci_eb":          (float(mt.group(8))
                                   if mt.group(8) is not None else float("nan")),
            })
            continue
        if RE_TRAJ_TRUNC.search(line):
            truncated = True
            continue
        ms = RE_SUMM_LINE.search(line)
        if ms:
            # Tokenize every key=value pair on the line so new engine fields
            # (ci_clust, ci_split, ci_eb, est_eb, n_blocks, act_outer, ...)
            # are captured without a regex change.
            final = {}
            for k, v in RE_KV.findall(ms.group(1)):
                try:
                    final[k] = float(v)
                except ValueError:
                    pass

    # delta_sec is derived from the C-measured cumulative elapsed_sec column.
    rounds.sort(key=lambda r: r["round"])
    prev = 0.0
    for r in rounds:
        r["delta_sec"] = max(0.0, r["elapsed_sec"] - prev)
        prev = r["elapsed_sec"]
    return rounds, final, truncated

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
        # Log-only capture: INFO must reach the server log...
        "SET log_min_messages = info;",
        # ...and we deliberately keep the client quiet, so the trajectory is
        # read ONLY from the log, never off this connection (Saketh model).
        "SET client_min_messages = warning;",
    ):
        cur.execute(stmt)
    cur.close()
    conn.commit()

def run_estimate(conn, join_sql, backend_pid):
    """Run the join under ROSL, drain its rows, then read this run's per-round
    trajectory + summary from the SERVER LOG.  Returns
    (rounds, call_wall_sec, n_rows, final, drain_cut).

    The client never reads conn.notices.  The C node dumps everything at
    executor teardown via elog(INFO, ...) to the log; we isolate our own lines
    with a unique sentinel pair plus our backend PID.  Row draining exists only
    to make the executor run to teardown (and to count rows as a sanity check);
    the row contents are never inspected.

    Draining uses a batched server-side cursor (ITERSIZE) so we are not paying a
    network round-trip per row, and a client-side wall-clock guard
    (DRAIN_TIMEOUT_S) bounds the loop independently of the server's
    statement_timeout.
    """
    run_id     = uuid.uuid4().hex
    open_mark  = f"ROSL_MARK_OPEN {run_id}"
    close_mark = f"ROSL_MARK_CLOSE {run_id}"

    # Resolve the active log per run (it may have rotated) and note where the
    # tail begins so we only read what this run appends.  We also record the
    # start mtime so that, if the file rotates mid-drain, we can find the newer
    # file(s) the teardown dump landed in (see _read_log_since).
    log_path     = _resolve_logfile(conn)
    start_offset = _logfile_size(log_path) if log_path else 0
    start_mtime  = time.time()

    mark_cur = conn.cursor()
    # Sentinels are emitted on THIS backend, so they land in the log bracketing
    # our teardown dump.  RAISE INFO requires log_min_messages <= info (set).
    mark_cur.execute("DO $$ BEGIN RAISE INFO '%s'; END $$;" % open_mark)

    sc = conn.cursor(name="rosl_cur")
    sc.itersize = ITERSIZE
    sc.execute(join_sql)

    t_start = time.time()
    n_rows = 0
    drain_cut = False
    for _row in sc:                 # contents ignored; we only need teardown
        n_rows += 1
        if (time.time() - t_start) >= DRAIN_TIMEOUT_S:
            drain_cut = True
            print("  WARNING: drain timeout hit -- closing cursor early. "
                  "Trajectory may be partial (executor teardown still dumps "
                  "what it has).", flush=True)
            break

    call_wall_sec = time.time() - t_start
    sc.close()                      # closing the portal triggers the teardown dump
    # Emit the closing sentinel AFTER the cursor closes, so it sits in the log
    # strictly after this run's ROSL_TRAJ/ROSL_SUMM lines.
    mark_cur.execute("DO $$ BEGIN RAISE INFO '%s'; END $$;" % close_mark)
    mark_cur.close()
    conn.commit()

    rounds, final, truncated = [], None, False
    if log_path is None:
        print("  WARNING: pg_current_logfile() returned empty -- logging "
              "collector off? Cannot read trajectory from server log.", flush=True)
        return rounds, call_wall_sec, n_rows, final, drain_cut

    # Wait briefly for the closing sentinel to appear, then parse the tail.
    # Read across a possible rotation: a long drain can roll the logfile over,
    # putting our teardown dump in a newer file than the one we started on.
    # If we can see our OPEN sentinel (so we are reading the right file set) but
    # the CLOSE has not flushed yet, extend the deadline -- on a loaded box the
    # teardown dump for a huge run can lag several seconds behind cursor close.
    deadline = time.time() + LOG_READ_WAIT_S
    extended = False
    text = ""
    while time.time() < deadline:
        text = _read_log_since(log_path, start_offset, start_mtime)
        if close_mark in text:
            break
        if (not extended) and (open_mark in text):
            deadline += LOG_READ_EXTRA_S
            extended = True
        time.sleep(LOG_POLL_S)

    rounds, final, truncated = _parse_rosl_log(text, backend_pid,
                                               open_mark, close_mark)
    if truncated:
        print("  WARNING: trajectory truncated in-engine (ROSL_TRAJ_CAP). "
              "Use a smaller scale factor.", flush=True)
    if not rounds and not drain_cut:
        saw_open  = open_mark in text
        saw_close = close_mark in text
        if saw_open and not saw_close:
            print("  WARNING: found our run's open marker but not the close in "
                  "the server log within the wait window -- teardown dump may "
                  "still be flushing (raise LOG_READ_WAIT_S/EXTRA) or the run "
                  "was very large.", flush=True)
        elif not saw_open:
            print("  WARNING: did not find our run's sentinels in the server log "
                  "-- likely a logfile rotation we could not follow, "
                  "logging_collector off, or log_min_messages != info. "
                  "Pinning log_rotation_size=0/log_rotation_age=0 avoids "
                  "rotation during long runs.", flush=True)
        else:
            print("  WARNING: sentinels present but no ROSL_TRAJ lines for our "
                  "PID -- check that enable_rosl produced a plain nested loop "
                  "and that the node actually ran.", flush=True)
    return rounds, call_wall_sec, n_rows, final, drain_cut

# ── entry point ───────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) != 7:
        print("Usage: python3 worker.py "
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

    # Backend PID identifies our lines in the shared server log.
    _pc = conn.cursor()
    _pc.execute("SELECT pg_backend_pid()")
    backend_pid = _pc.fetchone()[0]
    _pc.close()

    try:
        truth, truth_compute_sec = get_truth(conn, cache_path, key, schema, tA, tB, pred)
    except Exception as e:
        print(f"  [truth] FAILED: {e}", flush=True)
        conn.rollback()
        conn.close()
        sys.exit(1)

    with open(traj_path, "w") as traj_f, open(summ_path, "w") as summ_f:
        traj_f.write("size,query,zval,shuffle,repeat,round,pairs_seen,sample_matches,"
                     "mean_per_pair,est_join,ci_halfwidth,ci_eb,truth,ratio,rel_error,"
                     "elapsed_sec,delta_sec,pct_of_truth_output\n")
        # n_rows column: actual sample-match rows pulled through the cursor
        # (contents ignored; count is a sanity-check on streaming output).
        #
        # CI columns and their coverage flags:
        #   final_ci_halfwidth / ci_covers_truth
        #       the engine's self-normalized 95% CI.  FIXED-HORIZON-ONLY:
        #       coverage aggregated over repeats/shuffles is meaningful on
        #       runs that drain to completion, NOT on LIMIT/drain-cut stops
        #       (a data-dependent time outside the fixed-horizon guarantee).
        #   final_ci_eb / ci_eb_covers_truth
        #       empirical-Bernstein confidence sequence, centred on est_eb.
        #       Anytime-valid: the ONLY interval whose coverage is meaningful
        #       at a data-dependent stop -- filter on stop_reason accordingly.
        #   final_ci_clust / ci_clust_covers_truth, final_ci_split / ...
        #       block-clustered and split-half variants (M-block is the honest
        #       independence unit), centred on final_est.
        # All three extras are blank when an older engine build omits them.
        #
        # stop_reason: why the drain loop ended --
        #   limit          hit the query's output LIMIT cap
        #   drain_timeout  client-side DRAIN_TIMEOUT_S guard fired (partial run)
        #   exhausted      join output fully drained before any cap
        summ_f.write("size,query,zval,shuffle,repeat,truth,final_est,final_ratio,"
                     "final_ci_halfwidth,ci_covers_truth,"
                     "est_eb,final_ci_eb,ci_eb_covers_truth,"
                     "final_ci_clust,ci_clust_covers_truth,"
                     "final_ci_split,ci_split_covers_truth,"
                     "n_rounds,n_rows,stop_reason,"
                     "repeat_wall_sec,truth_compute_sec\n")

        configure_rosl(conn)

        cap = QUERY_LIMITS.get(q_name) if apply_limit else None
        n_ok = 0

        for rep in range(1, REPEATS + 1):
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"  [{ts}] estimate {key} repeat {rep}/{REPEATS}", flush=True)
            try:
                rounds, call_wall_sec, n_rows, final, drain_cut = run_estimate(
                    conn, join_sql, backend_pid)
            except psycopg2.errors.QueryCanceled as e:
                # Server-side statement_timeout (ROSL_TIMEOUT_S) fired.  The
                # statement was aborted rather than torn down normally, so no
                # summary row can be written; the manager's audit will flag
                # this job as missing.
                print(f"  [estimate] FAILED rep {rep}: server statement_timeout "
                      f"(ROSL_TIMEOUT_S={ROSL_TIMEOUT_S}s) cancelled the "
                      f"statement: {e}", flush=True)
                conn.rollback()
                continue
            except Exception as e:
                print(f"  [estimate] FAILED rep {rep}: {e}", flush=True)
                conn.rollback()
                continue

            if drain_cut:
                stop_reason = "drain_timeout"
            elif cap is not None and n_rows >= cap:
                stop_reason = "limit"
            else:
                stop_reason = "exhausted"

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
                # self-comparable within a single repeat.  With-replacement
                # sampling on high-multiplicity joins (e.g. Q11) can push this
                # past 100%.
                pct   = (r["sample_matches"] / truth * 100.0) if truth else float("nan")
                traj_f.write("%s,%s,%s,%s,%d,%d,%.0f,%d,%.10f,%.2f,%.2f,%.2f,%d,%.6f,%.6f,%.4f,%.4f,%.4f\n" % (
                    size, q_name, z_val, shuffle, rep, r["round"], r["pairs_seen"],
                    r["sample_matches"], r["mean_per_pair"], r["est_join"],
                    r["ci_halfwidth"], r["ci_eb"], truth, ratio, rerr,
                    r["elapsed_sec"], r["delta_sec"], pct))

            if rounds:
                # Prefer the engine's own ROSL_SUMM values (recomputed at
                # teardown against the exact act_outer) if captured; fall back
                # to the last trajectory round's.
                fe  = final.get("final_est_join", rounds[-1]["est_join"]) if final else rounds[-1]["est_join"]
                fci = final.get("ci_halfwidth",   rounds[-1]["ci_halfwidth"]) if final else rounds[-1]["ci_halfwidth"]
                fr  = (fe / truth) if truth else float("nan")
                covers = int(abs(fe - truth) <= fci) if truth else ""

                def _ci_cell(half, center):
                    """Return ('%.2f' % half, covers_flag) or ('', '') when the
                    engine did not emit this interval."""
                    if half is None or center is None or not truth:
                        return "", ""
                    return "%.2f" % half, int(abs(center - truth) <= half)

                # est_eb centres the EB confidence sequence (per the engine's
                # comments); the clustered/split intervals centre on fe.
                eeb = final.get("est_eb") if final else None
                eb_half, eb_cov       = _ci_cell(final.get("ci_eb") if final else None, eeb)
                clust_half, clust_cov = _ci_cell(final.get("ci_clust") if final else None, fe)
                split_half, split_cov = _ci_cell(final.get("ci_split") if final else None, fe)
                eeb_cell = ("%.2f" % eeb) if eeb is not None else ""

                summ_f.write("%s,%s,%s,%s,%d,%d,%.2f,%.6f,%.2f,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%s,%.3f,%.3f\n" % (
                    size, q_name, z_val, shuffle, rep, truth, fe, fr, fci, covers,
                    eeb_cell, eb_half, eb_cov, clust_half, clust_cov,
                    split_half, split_cov,
                    len(rounds), n_rows, stop_reason,
                    call_wall_sec, truth_compute_sec))
                n_ok += 1
                eb_note = (f"  eb={eeb_cell}+/-{eb_half} cov={eb_cov}"
                           if eb_half else "  (no ROSL_SUMM extras captured)")
                print(f"      final est={fe:,.0f} +/- {fci:,.0f}  truth={truth:,}  "
                      f"ratio={fr:.4f}  covered={covers}  stop={stop_reason}  "
                      f"rounds={len(rounds)}  n_rows={n_rows}  wall={call_wall_sec:.1f}s"
                      f"{eb_note}",
                      flush=True)
            else:
                print(f"      no ROSL_TRAJ lines parsed from server log "
                      f"(wall={call_wall_sec:.1f}s, stop={stop_reason}) -- is "
                      "enable_rosl wired, the plan a plain nested loop, and "
                      "log_min_messages=info?",
                      flush=True)

            traj_f.flush()
            summ_f.flush()

    conn.close()
    if n_ok < REPEATS:
        print(f"Done with FAILURES: {n_ok}/{REPEATS} repeats produced a summary "
              f"row. -> {prefix}.[traj|summ].csv", flush=True)
        sys.exit(3)   # nonzero so the manager records/retries this job
    print(f"Done. -> {prefix}.[traj|summ].csv", flush=True)

if __name__ == "__main__":
    main()