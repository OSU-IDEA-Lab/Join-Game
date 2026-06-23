#!/usr/bin/env python3
"""
Multi-scale TPC-H loader for the alt cluster.

Loads one DATABASE per scale factor -- db name "tpch{token}g" -- so the scales
coexist instead of clobbering each other (every scale's upload SQL targets the
same z0/z1/z1_5 schemas and starts with TRUNCATE, so they cannot share a DB).

For each scale it: ensures the database exists, creates the schemas/tables,
runs the scale's \\copy load, and builds the nine shuffle schemas.  Scales whose
database is already populated are skipped, so re-running is safe and the 10g set
(loaded earlier as `tpch`, then renamed to `tpch10g`) is never re-uploaded.

  NAMING: the 0.01 GB token is "0_01" (underscore) -> db "tpch0_01g", matching
  the upload-file names; "0.01" would be an illegal unquoted identifier.

Prereq: the alt postmaster must be up on PORT (see pg_ctl line printed on error).
"""
import subprocess
import sys
import datetime
import psycopg2
import os

# ── alt cluster connection ───────────────────────────────────────────────────
HOST   = "localhost"
PORT   = "1533"
USER   = "jinjo"

PREFIX = "/data/jinjo/alt"
PSQL   = os.path.join(PREFIX, "bin", "psql")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TABLES_SQL = os.path.join(SCRIPT_DIR, "tpch_tables.sql")

# ── scales to load: (size_token, upload_sql) -> database "tpch{token}g" ───────
SCALES = [
    ("01", "tpch_0_01g_upload.sql"),
    ("1",    "tpch_1g_upload.sql"),
    # ("10", "tpch_10g_upload.sql"),  # already loaded; rename tpch -> tpch10g instead
]

SKIP_IF_POPULATED = True     # skip a scale whose db already has z0.lineitem rows
SHUFFLE_SKEWS  = ["0", "1", "1_5"]
SHUFFLE_IDS    = ["1", "2", "3"]
SHUFFLE_TABLES = ["part", "orders", "lineitem", "supplier", "customer", "partsupp"]

def db_for(token):
    return f"tpch{token}g"

# ── psql / server helpers ────────────────────────────────────────────────────

def psql_run(db, args, capture=False):
    kw = dict(universal_newlines=True)
    if capture:
        kw.update(stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return subprocess.run([PSQL, "-h", HOST, "-p", PORT, "-d", db] + args, **kw)

def server_up():
    r = psql_run("postgres", ["-tAc", "SELECT 1"], capture=True)
    return r.returncode == 0 and r.stdout.strip() == "1"

def db_exists(db):
    r = psql_run("postgres", ["-tAc",
                 f"SELECT 1 FROM pg_database WHERE datname = '{db}'"], capture=True)
    return r.stdout.strip() == "1"

def ensure_database(db):
    if db_exists(db):
        print(f"  database {db} exists.")
        return
    print(f"  creating database {db} ...")
    if psql_run("postgres", ["-c", f"CREATE DATABASE {db}"]).returncode != 0:
        print(f"ERROR: could not create database {db}")
        sys.exit(1)

def is_populated(db):
    # True if z0.lineitem exists and has at least one row.
    r = psql_run(db, ["-tAc",
        "SELECT CASE WHEN to_regclass('z0.lineitem') IS NULL THEN 0 "
        "ELSE (SELECT count(*) FROM z0.lineitem) END"], capture=True)
    try:
        return int((r.stdout or "0").strip() or "0") > 0
    except ValueError:
        return False

def run_sql_file(db, label, sql_file):
    print(f"\n{'='*56}\n {label}  [{db}]\n{'='*56}")
    if psql_run(db, ["-f", sql_file]).returncode != 0:
        print(f"ERROR: {label} failed on {db}")
        sys.exit(1)

def create_shuffled_tables(db):
    conn = psycopg2.connect(host=HOST, port=PORT, dbname=db, user=USER)
    cur = conn.cursor()
    for sid in SHUFFLE_IDS:
        for skew in SHUFFLE_SKEWS:
            base, target = f"z{skew}", f"z{skew}_shuff{sid}"
            print(f"\n  shuffle -> {target} [{db}]")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {target};")
            for t in SHUFFLE_TABLES:
                ts = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"    [{ts}] {base}.{t} -> {target}.{t}")
                cur.execute(f"DROP TABLE IF EXISTS {target}.{t};")
                cur.execute(f"CREATE TABLE {target}.{t} AS "
                            f"SELECT * FROM {base}.{t} ORDER BY random();")
            conn.commit()
    conn.close()

# ── main ─────────────────────────────────────────────────────────────────────

def main():
    if not server_up():
        print(f"ERROR: alt server not reachable at {HOST}:{PORT}. Start it, e.g.:")
        print(f'  {PREFIX}/bin/pg_ctl -D {PREFIX}/data -o "-p {PORT}" -l /tmp/pg_alt.log start')
        sys.exit(1)

    for token, upload_sql in SCALES:
        db = db_for(token)
        upload_path = os.path.join(SCRIPT_DIR, upload_sql)
        print(f"\n########## SCALE {token}  ->  {db} ##########")

        ensure_database(db)

        if SKIP_IF_POPULATED and is_populated(db):
            print(f"  {db} already populated (z0.lineitem has rows) -- skipping. "
                  f"Set SKIP_IF_POPULATED=False to force a reload.")
            continue

        run_sql_file(db, "Creating schemas and tables", TABLES_SQL)
        run_sql_file(db, f"Loading {token} TPC-H data", upload_path)
        print(f"\n  shuffling {db} ...")
        create_shuffled_tables(db)
        print(f"  {db} done.")

    print("\nAll requested scales processed.")

if __name__ == "__main__":
    main()