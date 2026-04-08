-- Disable merge and nested loop joins to force a Hash Join
SET enable_mergejoin = off;
SET enable_nestloop = off;

-- 1. Print the execution plan to verify it is using a Hash Join
EXPLAIN SELECT a.brand, b.brand
FROM wdc1brands a
JOIN wdc1brands b
  ON a.brand = b.brand
LIMIT 50;

-- 2. Execute the actual query
SELECT a.brand, b.brand
FROM wdc1brands a
JOIN wdc1brands b
  ON a.brand = b.brand
LIMIT 50;