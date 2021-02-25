#!/bin/bash
export PATH=./exe/bin:$PATH

DB_PORT="5009"
DB_DIR=/Users/dsp/code/school/db-group/Join-Game/data
SQL_SCRIPT=/Users/dsp/code/school/db-group/Join-Game/utils/join_cfg.sql

# First stop the currently running server (if any)
pg_ctl -D $DB_DIR -o "-p ${DB_PORT}" stop

# Recompile & build
make
make install

# Restart the server with the fresh build
pg_ctl -D $DB_DIR -o "-p ${DB_PORT}" start

# Run the SQL script with psql client to observe the output
psql -p $DB_PORT -d tpch -f $SQL_SCRIPT