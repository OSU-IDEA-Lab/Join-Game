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
    nohup python3 test/tpch_manger.py 6_18 --workers 8 --limit > 6_18_rosl.log 2>&1 &
    nohup python3 test/tpch_manger.py 6_18 --workers 8 > 6_18_rosl.log 2>&1 &
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
SIZES    = ["01", "1"]              # tpch{size}g databases; add "10" if available
# SIZES    = ["01", "1", "10"]              # tpch{size}g databases; add "10" if available
ZVALS    = ["0", "1"]       # uniform -> increasingly skewed
# ZVALS    = ["0", "1", "1_5"]       # uniform -> increasingly skewed
SHUFFLES = ["1","2", "3"]         # repeated data layouts (variance)
QUERIES  = ["Q9", "Q10", "Q11", "Q12", "Q15"]

# ── per-query output limits (edit here to adjust; must stay in sync with worker) ──
# Limits from tpch_manager.py as of 5/27/2026.
QUERY_LIMITS = {
    "Q9":  221700,
    "Q10": 13000,
    "Q11": 327624700,
    "Q12": 1000,
    "Q15": 43800,
}

# Table/predicate definitions -- mirrors QUERIES dict in worker.py.
_QUERY_DEFS = {
    "Q9":  ("partsupp", "lineitem", "ps_partkey = l_partkey"),
    "Q10": ("customer", "orders",   "c_custkey = o_custkey"),
    "Q11": ("orders",   "lineitem", "o_orderdate = l_shipdate"),
    "Q12": ("orders",   "lineitem", "o_orderkey = l_orderkey"),
    "Q15": ("supplier", "lineitem", "s_suppkey = l_suppkey"),
}

def get_queries(q_name, z_val, shuffle, apply_limit):
    """Return the SQL string for one (query, schema, limit) combination.
    Mirrors tpch_manager.py's get_queries() so output limits are easy to
    find and adjust in one place.  Edit QUERY_LIMITS above to change caps."""
    if q_name not in _QUERY_DEFS:
        return None
    tA, tB, pred = _QUERY_DEFS[q_name]
    schema = f"z{z_val}_shuff{shuffle}"
    base   = f"SELECT * FROM {schema}.{tA}, {schema}.{tB} WHERE {pred}"
    if apply_limit and q_name in QUERY_LIMITS:
        return f"{base} LIMIT {QUERY_LIMITS[q_name]};"
    return f"{base};"


WORKER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "worker.py")

# ── subprocess runner (blocking; called from a thread-pool thread) ─────────────

def _run_worker(cmd, log_path):
    """Run one worker subprocess to completion, capturing its output to
    log_path.  Returns the wall-clock duration in seconds, so the manager can
    record a per-job timing row alongside the merged accuracy/efficiency
    CSVs (see job_timing.csv in _write_job_timing below)."""
    t0 = time.time()
    with open(log_path, 'w') as lf:
        subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT)
    wall_sec = time.time() - t0
    print(f"FINISHED: {log_path}  ({wall_sec:.1f}s)", flush=True)
    return wall_sec

# ── CSV merge (called after all workers finish) ───────────────────────────────

def _merge_csvs(pattern, out_path):
    """Concatenate all CSVs matching pattern into out_path, keeping one header row."""
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"  [merge] no files matched {pattern}", flush=True)
        return
    with open(out_path, 'w') as out:
        header_written = False
        for path in files:
            with open(path) as f:
                lines = f.readlines()
            if not lines:
                continue
            if not header_written:
                out.writelines(lines)       # first file: include header
                header_written = True
            else:
                out.writelines(lines[1:])   # subsequent files: skip repeated header
    print(f"  [merge] {len(files)} files -> {out_path}", flush=True)

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
    with open(out_path, 'w') as out:
        out.write("size,query,zval,shuffle,wall_sec,log_path\n")
        for fut, (size, q, z, shuffle, log_path) in futures.items():
            try:
                wall_sec = fut.result()
            except Exception as e:
                print(f"  [timing] job {q}_{size}_z{z}_sch{shuffle} raised: {e}", flush=True)
                wall_sec = float("nan")
            out.write(f"{size},{q},{z},{shuffle},{wall_sec:.3f},{log_path}\n")
    print(f"  [timing] {len(futures)} jobs -> {out_path}", flush=True)

# ── main ─────────────────────────────────────────────────────────────────────

def manage(results_dir, max_workers, apply_limits):
    os.makedirs(results_dir, exist_ok=True)

    combos = list(itertools.product(SIZES, ZVALS, SHUFFLES, QUERIES))
    limit_status = "ON" if apply_limits else "OFF"
    print(f"Queuing {len(combos)} jobs  |  max_workers={max_workers}  |  "
          f"Output limits: {limit_status}  |  results -> {results_dir}/", flush=True)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for size, z, shuffle, q in combos:
            log_path = os.path.join(results_dir, f"{q}_{size}_z{z}_sch{shuffle}.nohup.log")
            cmd = [
                sys.executable, WORKER_SCRIPT,
                size, q, z, shuffle, results_dir, "1" if apply_limits else "0",
            ]
            print(f"  QUEUED: {q} | size={size} | z={z} | sch={shuffle} | limit={limit_status}", flush=True)
            fut = executor.submit(_run_worker, cmd, log_path)
            futures[fut] = (size, q, z, shuffle, log_path)
    # ThreadPoolExecutor.__exit__ blocks until all submitted jobs finish.

    _write_job_timing(results_dir, futures)
    _merge_outputs(results_dir)
    print(f"\nAll done."
          f"\n  Trajectory  -> {results_dir}/trajectory.csv"
          f"\n  Summary     -> {results_dir}/summary.csv"
          f"\n  Job timing  -> {results_dir}/job_timing.csv"
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
    args = parser.parse_args()
    manage(args.results_dir, args.workers, args.limit)