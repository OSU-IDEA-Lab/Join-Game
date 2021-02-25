#!/bin/bash
export PATH=./exe/bin:$PATH

DB_DIR=/Users/dsp/code/school/db-group/Join-Game/data
SQL_SCRIPT=/Users/dsp/code/school/db-group/Join-Game/utils/join_cfg.sql

# First stop the currently running server (if any)
pg_ctl -D $DB_DIR stop

# Recompile & build
make
make install

# Restart the server with the fresh build
pg_ctl -D $DB_DIR start

# Run the SQL script with psql client to observe the output
psql -d tpch -f $SQL_SCRIPT