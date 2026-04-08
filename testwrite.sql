SET enable_mergejoin = off;
SET enable_nestloop = off;

EXPLAIN SELECT a.brand, b.brand
FROM wdc1brands a
JOIN wdc1brands b
  ON a.brand = b.brand
LIMIT 50;

-- Redirect all output after this line to a text file
\o query_output.txt

SELECT a.brand, b.brand
FROM wdc1brands a
JOIN wdc1brands b
  ON a.brand = b.brand
LIMIT 50;

-- Stop redirecting (optional if it's the end of the file)
\o