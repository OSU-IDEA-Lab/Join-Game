#!/usr/bin/env python3
"""
ROSL estimator-accuracy manager.

Fans out one worker subprocess per (size, z_val, shuffle, query) combination,
capping concurrency with a ThreadPoolExecutor.  Each worker writes its own
per-job .traj.csv and .summ.csv (now carrying both accuracy and per-round/
per-repeat efficiency columns -- see worker.py); the manager
merges them into the final trajectory.csv and summary.csv once all workers
finish.

The manager also times each worker subprocess end-to-end and writes
job_timing.csv -- one wall-clock row per (size, z, shuffle, query) job -- so
efficiency can be compared at the job level too, on top of the per-round and
per-repeat timing already inside each job's own CSVs.

Each worker's stdout/stderr is captured into a .nohup.log file in results_dir,
matching the convention from tpch_manager.py.

COMMUNICATION MODEL (log-only; see worker.py for detail)
--------------------------------------------------------
The ROSL C node dumps its per-round trajectory + summary to the PostgreSQL
SERVER LOG at executor teardown (elog INFO: ROSL_TRAJ / ROSL_SUMM); each worker
reads back only its own statement's lines via backend-PID + unique-sentinel
filtering.  This requires the alt cluster to have:
    logging_collector = on        (managed logfile exists on disk)
    log_min_messages  = info      (INFO actually reaches the log)
    log_line_prefix   includes %p (per-backend PID attribution)
The worker sets log_min_messages=info per session, but logging_collector is a
restart-only GUC and must already be on.  No measurement crosses the client
connection (client_min_messages is kept at warning), so the named-cursor
notice-flush deadlock from the previous design is structurally impossible.

LOG ROTATION ON LONG RUNS
-------------------------
Long-draining jobs (large scale, Q9/Q11) can run long enough that the collector
rotates the logfile mid-run (defaults: log_rotation_size=10MB, log_rotation_age
=1d), so the teardown ROSL_TRAJ/ROSL_SUMM dump lands in a NEWER file.  The
worker now follows rotation (it re-reads sibling log files touched since the run
started), but for the cleanest behaviour on a big sweep, pin a single logfile:
    log_rotation_size = 0
    log_rotation_age  = 0
then SELECT pg_reload_conf();  (these are SIGHUP-level, no restart needed).

Usage:
    nohup python3 test/tpch_manager.py [results_dir] [--workers N] [--limit] > [logfile_name] 2>&1 &

Examples:
    nohup python3 test/tpch_manager.py 6_18 --workers 8 --limit > 6_18_rosl.log 2>&1 &
    nohup python3 test/tpch_manager.py 6_18 --workers 8 > 6_18_rosl.log 2>&1 &
"""
import os
import sys
import time
import glob
import argparse
import itertools
import subprocess
from concurrent.futures import ThreadPoolExecutor

# ── sweep parameters (must match worker.py) ───────────────────
SIZES    = ["1"]            
# SIZES    = ["01", "1"]              
# SIZES    = ["01", "1", "10"]              # tpch{size}g databases
ZVALS    = ["0", "1", "1_5"]       # uniform -> increasingly skewed
# ZVALS    = ["1_5"]       # uniform -> increasingly skewed
SHUFFLES = ["1","2", "3"]         # repeated data layouts (variance)
QUERIES  = ["Q9", "Q10", "Q11", "Q12", "Q15"]

# ── single source of truth for query shapes and output limits ────────────────
#
# QUERY_DEFS maps query name -> (tables, predicate).  get_queries() below
# GENERATES the SQL from this table, so there is exactly one place to edit.
#
# worker.py and accuracy_charts.py must IMPORT these (from tpch_manager import
# QUERY_DEFS, QUERY_LIMITS, get_queries) instead of keeping mirror copies --
# the previous four hand-synced copies are how limit/predicate drift happens.
QUERY_DEFS = {
    # 2-relation
    "Q9":     (("partsupp", "lineitem"),             "ps_partkey = l_partkey"),
    "Q10":    (("customer", "orders"),               "c_custkey = o_custkey"),
    "Q11":    (("orders",   "lineitem"),             "o_orderdate = l_shipdate"),
    "Q12":    (("orders",   "lineitem"),             "o_orderkey = l_orderkey"),
    "Q15":    (("supplier", "lineitem"),             "s_suppkey = l_suppkey"),
    # 3-relation
    "Q2":     (("part", "supplier", "partsupp"),     "p_partkey = ps_partkey and s_suppkey = ps_suppkey"),
    "Q3":     (("customer", "orders", "lineitem"),   "c_custkey = o_custkey and o_orderkey = l_orderkey"),
    "Q5":     (("orders", "supplier", "lineitem"),   "s_suppkey = l_suppkey and o_orderkey = l_orderkey"),
    "Q8":     (("part", "supplier", "lineitem"),     "p_partkey = l_partkey and s_suppkey = l_suppkey"),
    "Q9_3R":  (("supplier", "partsupp", "lineitem"), "s_suppkey = l_suppkey and ps_suppkey = l_suppkey"),
    "test":   (("part", "supplier", "lineitem"),     "l_suppkey = s_suppkey and l_partkey = p_partkey"),
}

# Per-query output caps.  Provenance:
#   Q9/Q11        JoinLearning_SIGMOD_25 (= JoinGame_PVLDB_25; missing from JoinGame-SIGMOD-24-bk)
#   Q10/Q15/Q2/Q8 JoinLearning_SIGMOD_25 (= JoinGame_PVLDB_25; less than JoinGame-SIGMOD-24-bk)
#   Q12/Q3/Q5     JoinGame-SIGMOD-24-bk  (missing from the other two)
#   Q9_3R/test    no published limit -> always run un-capped.
#
# CAUTION -- these caps are absolute row counts taken from papers run at ONE
# scale factor; they do NOT scale with SIZES.  At size "01" (SF 0.01) Q11's cap
# (327.6M) exceeds anything the run can emit, so Q11 jobs only end at the
# worker's wall-clock timeout; Q12's cap (1000) is ~1.6% of the true output, so
# its estimate is starved.  manage() prints a per-job cap sanity line at queue
# time so this is visible up front.
QUERY_LIMITS = {
    "Q9":  221700,
    "Q10": 13000,
    "Q11": 327624700,
    "Q12": 1000,
    "Q15": 43800,
    "Q2":  5800,
    "Q3":  350,
    "Q5":  4000,
    "Q8":  43800,
}

def get_queries(q_name, val, shuff, limit):
    """Return [(SQL, num_relations, target_pct)] for q_name against the
    z{val}_shuff{shuff} schema, generated from QUERY_DEFS / QUERY_LIMITS.

    target_pct semantics preserved from the original hand-written version:
    100.00 when a cap is applied, 1.00 otherwise (including queries that have
    no published cap, which always run un-capped).
    """
    if q_name not in QUERY_DEFS:
        return []

    schema = f"z{val}_shuff{shuff}"
    tables, predicate = QUERY_DEFS[q_name]
    num_relations = len(tables)

    from_clause = ", ".join(f"{schema}.{t}" for t in tables)
    sql = f"select * from {from_clause} where {predicate}"

    cap = QUERY_LIMITS.get(q_name) if limit else None
    if cap is not None:
        sql += f" LIMIT {cap}"
        target_pct = 100.00
    else:
        target_pct = 1.00
    sql += ";"

    return [(sql, num_relations, target_pct)]
    
WORKER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "worker.py")

# ── subprocess runner (blocking; called from a thread-pool thread) ─────────────

def _run_worker(cmd, log_path, retries=0):
    """Run one worker subprocess to completion, capturing its output to
    log_path.  Returns (wall_sec, returncode, attempts).

    Unlike the previous version, a nonzero exit is no longer swallowed: it is
    printed loudly, optionally retried (--retries), and recorded in
    job_timing.csv, so a job that dies (e.g. the Q15 z=1 sch=3 hole in the
    6/18 sweep) can't vanish silently."""
    attempts = 0
    t0 = time.time()
    while True:
        attempts += 1
        mode = 'w' if attempts == 1 else 'a'
        with open(log_path, mode) as lf:
            if attempts > 1:
                lf.write(f"\n===== RETRY attempt {attempts} =====\n")
                lf.flush()
            proc = subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT)
        if proc.returncode == 0 or attempts > retries:
            break
        print(f"RETRYING (rc={proc.returncode}, attempt {attempts + 1}): {log_path}",
              flush=True)
    wall_sec = time.time() - t0
    status = "FINISHED" if proc.returncode == 0 else f"FAILED rc={proc.returncode}"
    print(f"{status}: {log_path}  ({wall_sec:.1f}s, {attempts} attempt(s))", flush=True)
    return wall_sec, proc.returncode, attempts

# ── CSV merge (called after all workers finish) ───────────────────────────────

def _merge_csvs(pattern, out_path):
    """Concatenate all CSVs matching pattern into out_path, keeping one header
    row.  Files whose header differs from the first file's are SKIPPED with a
    loud warning instead of silently splicing mismatched columns -- mixing
    worker versions (e.g. one that records ci_eb and one that doesn't) used to
    corrupt the merged file undetectably."""
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"  [merge] no files matched {pattern}", flush=True)
        return
    header = None
    skipped = []
    with open(out_path, 'w') as out:
        for path in files:
            with open(path) as f:
                lines = f.readlines()
            if not lines:
                print(f"  [merge] WARNING: {path} is empty, skipping", flush=True)
                continue
            if header is None:
                header = lines[0].strip()
                out.writelines(lines)       # first file: include header
            elif lines[0].strip() != header:
                skipped.append(path)
            else:
                out.writelines(lines[1:])   # subsequent files: skip repeated header
    if skipped:
        print(f"  [merge] WARNING: {len(skipped)} file(s) skipped due to header "
              f"mismatch (worker-version drift?):", flush=True)
        for p in skipped:
            print(f"           {p}", flush=True)
    print(f"  [merge] {len(files) - len(skipped)} files -> {out_path}", flush=True)

def _merge_outputs(results_dir):
    print("\nMerging per-job CSVs into final output files ...", flush=True)
    _merge_csvs(os.path.join(results_dir, "*.traj.csv"),
                os.path.join(results_dir, "trajectory.csv"))
    _merge_csvs(os.path.join(results_dir, "*.summ.csv"),
                os.path.join(results_dir, "summary.csv"))

# ── job-level timing (one row per (size, z, shuffle, query) job) ─────────────

def _write_job_timing(results_dir, futures):
    """Write job_timing.csv: one row per job with its total wall-clock time."""
    out_path = os.path.join(results_dir, "job_timing.csv")
    print("\nWriting per-job wall-clock timing ...", flush=True)
    n_failed = 0
    with open(out_path, 'w') as out:
        out.write("size,query,zval,shuffle,wall_sec,returncode,attempts,log_path\n")
        for fut, (size, q, z, shuffle, log_path) in futures.items():
            try:
                wall_sec, rc, attempts = fut.result()
            except Exception as e:
                print(f"  [timing] job {q}_{size}_z{z}_sch{shuffle} raised: {e}", flush=True)
                wall_sec, rc, attempts = float("nan"), -1, 0
            if rc != 0:
                n_failed += 1
            out.write(f"{size},{q},{z},{shuffle},{wall_sec:.3f},{rc},{attempts},{log_path}\n")
    print(f"  [timing] {len(futures)} jobs ({n_failed} failed) -> {out_path}", flush=True)
    return n_failed

# ── post-merge completeness audit ────────────────────────────────────────────

def _audit_summary(results_dir, combos):
    """Compare (size, query, zval, shuffle) rows present in the merged
    summary.csv against every queued combo.  A worker that exits 0 but writes
    no .summ.csv (or a merge that skips a file) still shows up here.  Missing
    combos are printed and written to missing_jobs.csv for easy re-queuing."""
    summ_path = os.path.join(results_dir, "summary.csv")
    print("\nAuditing merged summary against queued jobs ...", flush=True)
    if not os.path.exists(summ_path):
        print("  [audit] summary.csv missing entirely!", flush=True)
        return
    seen = set()
    with open(summ_path) as f:
        header = f.readline().strip().split(',')
        try:
            idx = [header.index(c) for c in ("size", "query", "zval", "shuffle")]
        except ValueError:
            print(f"  [audit] summary.csv lacks key columns; header = {header}",
                  flush=True)
            return
        for line in f:
            parts = line.rstrip("\n").split(',')
            if len(parts) >= len(header):
                seen.add(tuple(parts[i] for i in idx))
    missing = [(size, q, z, sh) for (size, z, sh, q) in combos
               if (size, q, z, sh) not in seen]
    if not missing:
        print(f"  [audit] OK: all {len(combos)} queued jobs present in summary.csv",
              flush=True)
        return
    miss_path = os.path.join(results_dir, "missing_jobs.csv")
    with open(miss_path, 'w') as out:
        out.write("size,query,zval,shuffle\n")
        for row in missing:
            out.write(",".join(row) + "\n")
            print(f"  [audit] MISSING: size={row[0]} {row[1]} z={row[2]} sch={row[3]}",
                  flush=True)
    print(f"  [audit] {len(missing)} of {len(combos)} jobs missing "
          f"-> {miss_path}", flush=True)

# ── main ─────────────────────────────────────────────────────────────────────

def manage(results_dir, max_workers, apply_limits, retries=0):
    os.makedirs(results_dir, exist_ok=True)

    combos = list(itertools.product(SIZES, ZVALS, SHUFFLES, QUERIES))
    limit_status = "ON" if apply_limits else "OFF"
    print(f"Queuing {len(combos)} jobs  |  max_workers={max_workers}  |  "
          f"Output limits: {limit_status}  |  retries={retries}  |  "
          f"results -> {results_dir}/", flush=True)

    if apply_limits and len(SIZES) > 1:
        print("WARNING: QUERY_LIMITS are absolute row counts fixed at one scale "
              "factor; sweeping multiple SIZES with --limit applies the SAME cap "
              "to every size (the sampled fraction of truth changes with SF).",
              flush=True)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for size, z, shuffle, q in combos:
            log_path = os.path.join(results_dir, f"{q}_{size}_z{z}_sch{shuffle}.nohup.log")
            cmd = [
                sys.executable, WORKER_SCRIPT,
                size, q, z, shuffle, results_dir, "1" if apply_limits else "0",
            ]
            cap = QUERY_LIMITS.get(q) if apply_limits else None
            cap_note = f"cap={cap}" if cap is not None else "cap=none"
            print(f"  QUEUED: {q} | size={size} | z={z} | sch={shuffle} | {cap_note}", flush=True)
            fut = executor.submit(_run_worker, cmd, log_path, retries)
            futures[fut] = (size, q, z, shuffle, log_path)
    # ThreadPoolExecutor.__exit__ blocks until all submitted jobs finish.

    n_failed = _write_job_timing(results_dir, futures)
    _merge_outputs(results_dir)
    _audit_summary(results_dir, combos)
    if n_failed:
        print(f"\nWARNING: {n_failed} job(s) exited nonzero -- see job_timing.csv "
              f"and the matching .nohup.log files.", flush=True)
    print(f"\nAll done."
          f"\n  Trajectory  -> {results_dir}/trajectory.csv"
          f"\n  Summary     -> {results_dir}/summary.csv"
          f"\n  Job timing  -> {results_dir}/job_timing.csv"
          f"\n  Job audit   -> {results_dir}/missing_jobs.csv (only if jobs are missing)"
          f"\n  Truth cache -> {results_dir}/truth_counts.json"
          f"\n  Worker logs -> {results_dir}/*.nohup.log", flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ROSL estimator accuracy benchmark manager")
    parser.add_argument(
        "results_dir", nargs="?", default="rosl_estimator_results",
        help="Output directory (default: rosl_estimator_results)")
    parser.add_argument(
        "--workers", type=int, default=4,
        help="Max concurrent worker processes (default: 4)")
    parser.add_argument(
        "--limit", action="store_true",
        help="Apply output LIMIT constraints to queries (edit QUERY_LIMITS in this file to adjust)")
    parser.add_argument(
        "--retries", type=int, default=0,
        help="Times to re-run a worker that exits nonzero (default: 0)")
    args = parser.parse_args()
    manage(args.results_dir, args.workers, args.limit, args.retries)