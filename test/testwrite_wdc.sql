SET enable_nestloop = off;
SET enable_mergejoin = on;
SET max_parallel_workers_per_gather = 0;
SET work_mem = '1GB';
SET client_min_messages = info;

DROP TABLE IF EXISTS limited_brands;
CREATE TEMP TABLE limited_brands AS 
SELECT * FROM wdc1brands LIMIT 10000;

\o test/query_output.txt

-- 1. Print the execution plan for Merge Join
EXPLAIN SELECT COUNT(*)
FROM limited_brands a
JOIN limited_brands b 
  ON a.brand = b.brand 
JOIN limited_brands c 
  ON a.brand = c.brand 
  AND b.brand = c.brand;

-- 2. Execute and count for Merge Join
SELECT COUNT(*)
FROM limited_brands a
JOIN limited_brands b 
  ON a.brand = b.brand 
JOIN limited_brands c 
  ON a.brand = c.brand 
  AND b.brand = c.brand;

SET enable_mergejoin = off;

-- 3. Print the execution plan for EHJ
EXPLAIN SELECT COUNT(*)
FROM limited_brands a
JOIN limited_brands b 
  ON a.brand = b.brand 
JOIN limited_brands c 
  ON a.brand = c.brand 
  AND b.brand = c.brand;

-- 4. Execute and count for EHJ
SELECT COUNT(*)
FROM limited_brands a
JOIN limited_brands b 
  ON a.brand = b.brand 
JOIN limited_brands c 
  ON a.brand = c.brand 
  AND b.brand = c.brand;

\o