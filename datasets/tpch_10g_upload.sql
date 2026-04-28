TRUNCATE 
    z0.region, z0.nation, z0.supplier, z0.customer, z0.part, z0.partsupp, z0.orders, z0.lineitem,
    z1.region, z1.nation, z1.supplier, z1.customer, z1.part, z1.partsupp, z1.orders, z1.lineitem,
    z1_5.region, z1_5.nation, z1_5.supplier, z1_5.customer, z1_5.part, z1_5.partsupp, z1_5.orders, z1_5.lineitem
RESTART IDENTITY CASCADE;

-- Populating z15 (Uniform Data)
\copy z0.region   FROM '/data/mettas/Join-Game/data/TPC-H/10/z0/region.tbl'   WITH (FORMAT csv, DELIMITER '|');
\copy z0.nation   FROM '/data/mettas/Join-Game/data/TPC-H/10/z0/nation.tbl'   WITH (FORMAT csv, DELIMITER '|');
\copy z0.customer FROM '/data/mettas/Join-Game/data/TPC-H/10/z0/customer.tbl' WITH (FORMAT csv, DELIMITER '|');
\copy z0.supplier FROM '/data/mettas/Join-Game/data/TPC-H/10/z0/supplier.tbl' WITH (FORMAT csv, DELIMITER '|');
\copy z0.part     FROM '/data/mettas/Join-Game/data/TPC-H/10/z0/part.tbl'     WITH (FORMAT csv, DELIMITER '|');
\copy z0.partsupp FROM '/data/mettas/Join-Game/data/TPC-H/10/z0/partsupp.tbl' WITH (FORMAT csv, DELIMITER '|');
\copy z0.orders   FROM '/data/mettas/Join-Game/data/TPC-H/10/z0/order.tbl'    WITH (FORMAT csv, DELIMITER '|');
\copy z0.lineitem FROM '/data/mettas/Join-Game/data/TPC-H/10/z0/lineitem.tbl' WITH (FORMAT csv, DELIMITER '|');

-- Populating z1 (Uniform Data)
\copy z1.region   FROM '/data/mettas/Join-Game/data/TPC-H/10/z1/region.tbl'   WITH (FORMAT csv, DELIMITER '|');
\copy z1.nation   FROM '/data/mettas/Join-Game/data/TPC-H/10/z1/nation.tbl'   WITH (FORMAT csv, DELIMITER '|');
\copy z1.customer FROM '/data/mettas/Join-Game/data/TPC-H/10/z1/customer.tbl' WITH (FORMAT csv, DELIMITER '|');
\copy z1.supplier FROM '/data/mettas/Join-Game/data/TPC-H/10/z1/supplier.tbl' WITH (FORMAT csv, DELIMITER '|');
\copy z1.part     FROM '/data/mettas/Join-Game/data/TPC-H/10/z1/part.tbl'     WITH (FORMAT csv, DELIMITER '|');
\copy z1.partsupp FROM '/data/mettas/Join-Game/data/TPC-H/10/z1/partsupp.tbl' WITH (FORMAT csv, DELIMITER '|');
\copy z1.orders   FROM '/data/mettas/Join-Game/data/TPC-H/10/z1/order.tbl'    WITH (FORMAT csv, DELIMITER '|');
\copy z1.lineitem FROM '/data/mettas/Join-Game/data/TPC-H/10/z1/lineitem.tbl' WITH (FORMAT csv, DELIMITER '|');

-- Populating z1_5 (Uniform Data)
\copy z1_5.region   FROM '/data/mettas/Join-Game/data/TPC-H/10/z1_5/region.tbl'   WITH (FORMAT csv, DELIMITER '|');
\copy z1_5.nation   FROM '/data/mettas/Join-Game/data/TPC-H/10/z1_5/nation.tbl'   WITH (FORMAT csv, DELIMITER '|');
\copy z1_5.customer FROM '/data/mettas/Join-Game/data/TPC-H/10/z1_5/customer.tbl' WITH (FORMAT csv, DELIMITER '|');
\copy z1_5.supplier FROM '/data/mettas/Join-Game/data/TPC-H/10/z1_5/supplier.tbl' WITH (FORMAT csv, DELIMITER '|');
\copy z1_5.part     FROM '/data/mettas/Join-Game/data/TPC-H/10/z1_5/part.tbl'     WITH (FORMAT csv, DELIMITER '|');
\copy z1_5.partsupp FROM '/data/mettas/Join-Game/data/TPC-H/10/z1_5/partsupp.tbl' WITH (FORMAT csv, DELIMITER '|');
\copy z1_5.orders   FROM '/data/mettas/Join-Game/data/TPC-H/10/z1_5/order.tbl'    WITH (FORMAT csv, DELIMITER '|');
\copy z1_5.lineitem FROM '/data/mettas/Join-Game/data/TPC-H/10/z1_5/lineitem.tbl' WITH (FORMAT csv, DELIMITER '|');