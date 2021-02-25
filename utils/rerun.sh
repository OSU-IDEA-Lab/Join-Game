#!/bin/bash
export PATH=./exe/bin:$PATH

# First stop the currently running server (if any)
pg_ctl -D ./data stop

# Recompile & build
make
make install

# Restart the server with the fresh build
pg_ctl -D ./data start

# Run the SQL script with psql client to observe the output
psql -d tpch -f ./data_loading/join.sql > results.txt