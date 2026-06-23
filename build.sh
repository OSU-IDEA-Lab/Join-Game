#!/bin/bash
set -e
PREFIX_DIR="/data/jinjo/alt"
DATA_DIR="/data/jinjo/alt/data"

cd "$(dirname "$0")"          # always build the tree this script lives in

./configure --prefix="$PREFIX_DIR" --enable-depend --enable-cassert --enable-debug
make
make install

# initialise a cluster only if one isn't already there
if [ ! -d "$DATA_DIR" ] || [ -z "$(ls -A "$DATA_DIR" 2>/dev/null)" ]; then
    "$PREFIX_DIR/bin/initdb" -D "$DATA_DIR"
else
    echo "Data dir $DATA_DIR exists and is non-empty -- skipping initdb."
fi

/data/jinjo/alt/bin/pg_ctl -D /data/jinjo/alt/data -o "-p 1533" -l /data/jinjo/alt/data/logfile start