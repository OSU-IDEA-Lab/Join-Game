SET enable_nestloop = off;
SET enable_mergejoin = on;
SET max_parallel_workers_per_gather = 0;
SET work_mem = '4MB';
SET client_min_messages = info;

\o query_output.txt

EXPLAIN SELECT COUNT(*)
FROM generate_series(1, 1000000) a(id)
JOIN generate_series(1, 1000000) b(id)
  ON a.id = b.id;

SELECT COUNT(*)
FROM generate_series(1, 1000000) a(id)
JOIN generate_series(1, 1000000) b(id)
  ON a.id = b.id;

SET enable_mergejoin = off;

EXPLAIN SELECT COUNT(*)
FROM generate_series(1, 1000000) a(id)
JOIN generate_series(1, 1000000) b(id)
  ON a.id = b.id;

SELECT COUNT(*)
FROM generate_series(1, 1000000) a(id)
JOIN generate_series(1, 1000000) b(id)
  ON a.id = b.id;

\o