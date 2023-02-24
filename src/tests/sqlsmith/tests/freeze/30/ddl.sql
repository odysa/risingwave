CREATE TABLE supplier (s_suppkey INT, s_name CHARACTER VARYING, s_address CHARACTER VARYING, s_nationkey INT, s_phone CHARACTER VARYING, s_acctbal NUMERIC, s_comment CHARACTER VARYING, PRIMARY KEY (s_suppkey));
CREATE TABLE part (p_partkey INT, p_name CHARACTER VARYING, p_mfgr CHARACTER VARYING, p_brand CHARACTER VARYING, p_type CHARACTER VARYING, p_size INT, p_container CHARACTER VARYING, p_retailprice NUMERIC, p_comment CHARACTER VARYING, PRIMARY KEY (p_partkey));
CREATE TABLE partsupp (ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost NUMERIC, ps_comment CHARACTER VARYING, PRIMARY KEY (ps_partkey, ps_suppkey));
CREATE TABLE customer (c_custkey INT, c_name CHARACTER VARYING, c_address CHARACTER VARYING, c_nationkey INT, c_phone CHARACTER VARYING, c_acctbal NUMERIC, c_mktsegment CHARACTER VARYING, c_comment CHARACTER VARYING, PRIMARY KEY (c_custkey));
CREATE TABLE orders (o_orderkey BIGINT, o_custkey INT, o_orderstatus CHARACTER VARYING, o_totalprice NUMERIC, o_orderdate DATE, o_orderpriority CHARACTER VARYING, o_clerk CHARACTER VARYING, o_shippriority INT, o_comment CHARACTER VARYING, PRIMARY KEY (o_orderkey));
CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity NUMERIC, l_extendedprice NUMERIC, l_discount NUMERIC, l_tax NUMERIC, l_returnflag CHARACTER VARYING, l_linestatus CHARACTER VARYING, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct CHARACTER VARYING, l_shipmode CHARACTER VARYING, l_comment CHARACTER VARYING, PRIMARY KEY (l_orderkey, l_linenumber));
CREATE TABLE nation (n_nationkey INT, n_name CHARACTER VARYING, n_regionkey INT, n_comment CHARACTER VARYING, PRIMARY KEY (n_nationkey));
CREATE TABLE region (r_regionkey INT, r_name CHARACTER VARYING, r_comment CHARACTER VARYING, PRIMARY KEY (r_regionkey));
CREATE TABLE person (id BIGINT, name CHARACTER VARYING, email_address CHARACTER VARYING, credit_card CHARACTER VARYING, city CHARACTER VARYING, state CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE auction (id BIGINT, item_name CHARACTER VARYING, description CHARACTER VARYING, initial_bid BIGINT, reserve BIGINT, date_time TIMESTAMP, expires TIMESTAMP, seller BIGINT, category BIGINT, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel CHARACTER VARYING, url CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING);
CREATE TABLE alltypes1 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE TABLE alltypes2 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE MATERIALIZED VIEW m0 AS SELECT t_1.extra AS col_0 FROM orders AS t_0 JOIN person AS t_1 ON t_0.o_clerk = t_1.city AND ((INT '-428824745') >= (SMALLINT '998')) WHERE CAST((INT '-2147483648') AS BOOLEAN) GROUP BY t_1.id, t_1.extra, t_1.city, t_0.o_orderkey, t_0.o_totalprice, t_0.o_comment;
CREATE MATERIALIZED VIEW m1 AS SELECT ((INT '953') % (BIGINT '9223372036854775807')) AS col_0 FROM (SELECT t_1.id AS col_0 FROM lineitem AS t_0 JOIN person AS t_1 ON t_0.l_returnflag = t_1.name AND true GROUP BY t_0.l_discount, t_0.l_extendedprice, t_1.name, t_1.email_address, t_1.id, t_0.l_orderkey, t_0.l_partkey) AS sq_2 WHERE false GROUP BY sq_2.col_0 HAVING true;
CREATE MATERIALIZED VIEW m2 AS SELECT (CAST(NULL AS STRUCT<a INT>)) AS col_0, hop_0.c8 AS col_1 FROM hop(alltypes2, alltypes2.c11, INTERVAL '3600', INTERVAL '43200') AS hop_0 GROUP BY hop_0.c15, hop_0.c8, hop_0.c14, hop_0.c6, hop_0.c9, hop_0.c16;
CREATE MATERIALIZED VIEW m3 AS SELECT ((t_0.n_regionkey + (SMALLINT '2213')) | t_0.n_regionkey) AS col_0 FROM nation AS t_0 FULL JOIN m0 AS t_1 ON t_0.n_name = t_1.col_0 AND true WHERE false GROUP BY t_0.n_regionkey, t_1.col_0 HAVING false;
CREATE MATERIALIZED VIEW m4 AS SELECT t_2.col_0 AS col_0, t_2.col_0 AS col_1, (CASE WHEN true THEN (BIGINT '13') WHEN (false) THEN (BIGINT '-6651847141933437193') ELSE ((INT '599') % t_2.col_0) END) AS col_2, ((INT '508') * t_2.col_0) AS col_3 FROM m1 AS t_2 WHERE ((REAL '732') > (INT '132')) GROUP BY t_2.col_0 HAVING false;
CREATE MATERIALIZED VIEW m5 AS SELECT 'PRTDq605Vs' AS col_0, 'DbZcevQ9Jp' AS col_1, (INTERVAL '3600') AS col_2, (247) AS col_3 FROM supplier AS t_2 GROUP BY t_2.s_name, t_2.s_phone HAVING false;
CREATE MATERIALIZED VIEW m6 AS WITH with_0 AS (SELECT t_2.l_returnflag AS col_0 FROM auction AS t_1 LEFT JOIN lineitem AS t_2 ON t_1.item_name = t_2.l_shipinstruct WHERE (true) GROUP BY t_2.l_returnflag HAVING CAST((INT '364') AS BOOLEAN)) SELECT (FLOAT '2147483647') AS col_0, (TIME '12:24:57' - (INTERVAL '1')) AS col_1, ((FLOAT '994') * (INTERVAL '604800')) AS col_2 FROM with_0;
CREATE MATERIALIZED VIEW m8 AS SELECT (- (FLOAT '710')) AS col_0, sq_2.col_0 AS col_1, sq_2.col_0 AS col_2, sq_2.col_0 AS col_3 FROM (SELECT t_1.extra AS col_0, TIME '12:23:58' AS col_1 FROM partsupp AS t_0 FULL JOIN bid AS t_1 ON t_0.ps_comment = t_1.extra WHERE true GROUP BY t_1.auction, t_1.date_time, t_1.extra, t_1.bidder, t_0.ps_comment, t_1.channel) AS sq_2 GROUP BY sq_2.col_0;
CREATE MATERIALIZED VIEW m9 AS SELECT 'QrbFdycSTt' AS col_0 FROM hop(person, person.date_time, INTERVAL '60', INTERVAL '1620') AS hop_0 GROUP BY hop_0.credit_card HAVING false;