#!/bin/bash


/data/jinjo/alt/bin/pg_ctl -D /data/jinjo/alt/data -o "-p 1533" -l /data/jinjo/alt/data/logfile stop 

make
make install

/data/jinjo/alt/bin/pg_ctl -D /data/jinjo/alt/data -o "-p 1533" -l /data/jinjo/alt/data/logfile start