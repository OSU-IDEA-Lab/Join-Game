#!/bin/bash

### Join-Game experimentation pipeline

## Install PSQL
# Configure
./configure --prefix=/Users/dsp/code/school/db-group/Join-Game/exe --enable-depend --enable-cassert --enable-debug

# Build & install
make
make install

# Make sure all binaries are accessible from root
export PATH=./exe/bin:$PATH

## Generate data
# Clone Skewed data generator & run `make` to install
# Rename `@` to `dbgen`
./dbgen -s 0.01 -z 0

# Remove trailing | using sed
sed 's/.$//' order.tbl > order_cl.tbl
sed 's/.$//' customer.tbl > customer_cl.tbl

## Setup database
# Create a database cluster directory
initdb -D ./data

# Start/ stop server 
exe/bin/pg_ctl -D ./data start
exe/bin/pg_ctl -D ./data stop

# Load data into DB
python data_loading/load_data.py  # Verify tbl paths

## Verify DB & run queries
# Open PostgreSQL client (psql)
pg_ctl -D ./data start  # This dir has tables loaded

# Start psql client 
psql tpch  # Connects to tpch db
\dt  # see all tables


## Experiments