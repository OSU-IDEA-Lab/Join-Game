SELECT 'z0.region' AS table_name, COUNT(*) FROM z0.region UNION ALL
SELECT 'z0.nation', COUNT(*) FROM z0.nation UNION ALL
SELECT 'z0.supplier', COUNT(*) FROM z0.supplier UNION ALL
SELECT 'z0.customer', COUNT(*) FROM z0.customer UNION ALL
SELECT 'z0.part', COUNT(*) FROM z0.part UNION ALL
SELECT 'z0.partsupp', COUNT(*) FROM z0.partsupp UNION ALL
SELECT 'z0.orders', COUNT(*) FROM z0.orders UNION ALL
SELECT 'z0.lineitem', COUNT(*) FROM z0.lineitem UNION ALL

SELECT 'z1.region', COUNT(*) FROM z1.region UNION ALL
SELECT 'z1.nation', COUNT(*) FROM z1.nation UNION ALL
SELECT 'z1.supplier', COUNT(*) FROM z1.supplier UNION ALL
SELECT 'z1.customer', COUNT(*) FROM z1.customer UNION ALL
SELECT 'z1.part', COUNT(*) FROM z1.part UNION ALL
SELECT 'z1.partsupp', COUNT(*) FROM z1.partsupp UNION ALL
SELECT 'z1.orders', COUNT(*) FROM z1.orders UNION ALL
SELECT 'z1.lineitem', COUNT(*) FROM z1.lineitem UNION ALL

SELECT 'z1_5.region', COUNT(*) FROM z1_5.region UNION ALL
SELECT 'z1_5.nation', COUNT(*) FROM z1_5.nation UNION ALL
SELECT 'z1_5.supplier', COUNT(*) FROM z1_5.supplier UNION ALL
SELECT 'z1_5.customer', COUNT(*) FROM z1_5.customer UNION ALL
SELECT 'z1_5.part', COUNT(*) FROM z1_5.part UNION ALL
SELECT 'z1_5.partsupp', COUNT(*) FROM z1_5.partsupp UNION ALL
SELECT 'z1_5.orders', COUNT(*) FROM z1_5.orders UNION ALL
SELECT 'z1_5.lineitem', COUNT(*) FROM z1_5.lineitem;