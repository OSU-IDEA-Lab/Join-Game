SET enable_mergejoin = off;
SET enable_nestloop = off;
SET max_parallel_workers_per_gather = 0;
SET work_mem = '256MB';
SET client_min_messages = info;

-- 1. Print the execution plan
EXPLAIN SELECT COUNT(*)
FROM wdc1brands a
JOIN wdc1brands b
  ON a.brand = b.brand;

-- 2. Execute the full table join
SELECT COUNT(*)
FROM wdc1brands a
JOIN wdc1brands b
  ON a.brand = b.brand;