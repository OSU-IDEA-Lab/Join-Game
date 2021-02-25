set enable_material = off;
set enable_mergejoin = off;
set enable_hashjoin = off;
set enable_indexonlyscan = off; 
set enable_indexscan = off;
set enable_bitmapscan=off;
set work_mem = "64kB";
set enable_seqscan = off;
set enable_fastjoin = on;
set enable_block = on;
set max_parallel_workers_per_gather=0;

EXPLAIN select * from customers join orders on customers.feature2 = orders.order_id;
select * from customers join orders on customers.feature2 = orders.order_id;