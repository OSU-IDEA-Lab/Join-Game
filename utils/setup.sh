#!/bin/bash

# Global variables
ROOT_DIR="/Users/dsp/code/school/db-group/Join-Game"
SRC_DIR="${ROOT_DIR}/exe"
DB_DIR="${ROOT_DIR}/data"

DB_USER="dsp"
DB_NAME="tpch"
DB_HOST="localhost"
DB_PORT="5009"

### Join-Game experimentation pipeline

## Install PSQL
# Configure
./configure --prefix=$SRC_DIR --enable-depend --enable-cassert --enable-debug

# Build & install
make
make install

# Make sure all binaries are accessible from root
export PATH="${SRC_DIR}/bin":$PATH

## Generate data
# Clone Skewed data generator
cd $ROOT_DIR
git clone https://github.com/gunaprsd/SkewedDataGenerator.git

# Install
cd SkewedDataGenerator
make

# Rename `@` to `dbgen`
mv @ dbgen

# Generate data with size(s) & skew(z)
"${ROOT_DIR}/SkewedDataGenerator/dbgen" -s 0.01 -z 0

# Remove trailing `|` so the data can be loaded into DB
sed 's/.$//' order.tbl > "${ROOT_DIR}/order.tbl"
sed 's/.$//' customer.tbl > "${ROOT_DIR}/customer.tbl"

cd $ROOT_DIR

## Setup database
# Create a database cluster directory
initdb -D $DB_DIR

# Start/ stop server 
pg_ctl -D $DB_DIR -o "-p ${DB_PORT}" start
pg_ctl -D $DB_DIR -o "-p ${DB_PORT}" stop

# Load data into DB
python "${ROOT_DIR}/utils/load_data_to_db.py" --cust "${ROOT_DIR}/customer.tbl" \
                                      --order "${ROOT_DIR}/order.tbl" \
                                      --db_host $DB_HOST \
                                      --db_port $DB_PORT \
                                      --db_user $DB_USER \
                                      --db_name $DB_NAME

## Verify DB & run queries
# Open PostgreSQL client (psql)
pg_ctl -D $DB_DIR start  # This dir has tables loaded

# Start psql client 
psql tpch  # Connects to tpch db
\dt  # see all tables

## Experiments