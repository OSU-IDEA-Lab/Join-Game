-- Create schemas to organize data by skew level
CREATE SCHEMA IF NOT EXISTS z0;
CREATE SCHEMA IF NOT EXISTS z1;
CREATE SCHEMA IF NOT EXISTS z1_5;

-- Helper function to create all TPC-H tables in a target schema
-- This avoids repeating the long CREATE TABLE blocks three times
DO $$
DECLARE
    sch text;
    schemas text[] := ARRAY['z0', 'z1', 'z1_5'];
BEGIN
    FOREACH sch IN ARRAY schemas LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS %I.nation (
            n_nationkey  INT,
            n_name       CHAR(25),
            n_regionkey  INT,
            n_comment    VARCHAR(152),
            n_dummy      VARCHAR(10),
            PRIMARY KEY (n_nationkey))', sch);

        EXECUTE format('CREATE TABLE IF NOT EXISTS %I.region (
            r_regionkey  INT,
            r_name       CHAR(25),
            r_comment    VARCHAR(152),
            r_dummy      VARCHAR(10),
            PRIMARY KEY (r_regionkey))', sch);

        EXECUTE format('CREATE TABLE IF NOT EXISTS %I.supplier (
            s_suppkey     INT,
            s_name        CHAR(25),
            s_address     VARCHAR(40),
            s_nationkey   INT,
            s_phone       CHAR(15),
            s_acctbal     DECIMAL(15,2),
            s_comment     VARCHAR(101),
            s_dummy       VARCHAR(10),
            PRIMARY KEY (s_suppkey))', sch);

        EXECUTE format('CREATE TABLE IF NOT EXISTS %I.customer (
            c_custkey     INT,
            c_name        VARCHAR(25),
            c_address     VARCHAR(40),
            c_nationkey   INT,
            c_phone       CHAR(15),
            c_acctbal     DECIMAL(15,2),
            c_mktsegment  CHAR(10),
            c_comment     VARCHAR(117),
            c_dummy       VARCHAR(10),
            PRIMARY KEY (c_custkey))', sch);

        EXECUTE format('CREATE TABLE IF NOT EXISTS %I.part (
            p_partkey     INT,
            p_name        VARCHAR(55),
            p_mfgr        CHAR(25),
            p_brand       CHAR(10),
            p_type        VARCHAR(25),
            p_size        INT,
            p_container   CHAR(10),
            p_retailprice DECIMAL(15,2),
            p_comment     VARCHAR(23),
            p_dummy       VARCHAR(10),
            PRIMARY KEY (p_partkey))', sch);

        EXECUTE format('CREATE TABLE IF NOT EXISTS %I.partsupp (
            ps_partkey     INT,
            ps_suppkey     INT,
            ps_availqty    INT,
            ps_supplycost  DECIMAL(15,2),
            ps_comment     VARCHAR(199),
            ps_dummy       VARCHAR(10),
            PRIMARY KEY (ps_partkey, ps_suppkey))', sch);

        EXECUTE format('CREATE TABLE IF NOT EXISTS %I.orders (
            o_orderkey       INT,
            o_custkey        INT,
            o_orderstatus    CHAR(1),
            o_totalprice     DECIMAL(15,2),
            o_orderdate      DATE,
            o_orderpriority  CHAR(15),
            o_clerk          CHAR(15),
            o_shippriority   INT,
            o_comment        VARCHAR(79),
            o_dummy          VARCHAR(10),
            PRIMARY KEY (o_orderkey))', sch);

        EXECUTE format('CREATE TABLE IF NOT EXISTS %I.lineitem (
            l_orderkey       INT,
            l_partkey        INT,
            l_suppkey        INT,
            l_linenumber     INT,
            l_quantity       DECIMAL(15,2),
            l_extendedprice  DECIMAL(15,2),
            l_discount       DECIMAL(15,2),
            l_tax            DECIMAL(15,2),
            l_returnflag     CHAR(1),
            l_linestatus     CHAR(1),
            l_shipdate       DATE,
            l_commitdate     DATE,
            l_receiptdate    DATE,
            l_shipinstruct   CHAR(25),
            l_shipmode       CHAR(10),
            l_comment        VARCHAR(44),
            l_dummy          VARCHAR(10))', sch);
    END LOOP;
END $$;