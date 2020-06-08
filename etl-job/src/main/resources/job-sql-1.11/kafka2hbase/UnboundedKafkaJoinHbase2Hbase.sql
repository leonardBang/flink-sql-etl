CREATE TABLE orders (
  order_id STRING,
  item    STRING,
  currency STRING,
  amount INT,
  order_time TIMESTAMP(3),
  proc_time as PROCTIME(),
  amount_kg as amount * 1000,
  ts as order_time + INTERVAL '1' SECOND,
  WATERMARK FOR order_time AS order_time
) WITH (
  'connector' = 'kafka',
  'topic' = 'flink_orders2',
  'properties.zookeeper.connect' = 'localhost:2181',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup3',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE currency (
  currency_id BIGINT,
  currency_name STRING,
  rate DOUBLE,
  currency_time TIMESTAMP(3),
  country STRING,
  timestamp9 TIMESTAMP(3),
  time9 TIME(3),
  gdp DECIMAL(38, 18)
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/test',
   'username' = 'root',
   'table-name' = 'currency',
   'password' = '',
   'driver' = 'com.mysql.jdbc.Driver',
   'lookup.cache.max-rows' = '500',
   'lookup.cache.ttl' = '10s',
   'lookup.max-retries' = '3');

CREATE TABLE country (
  rowkey VARCHAR,
  f1 ROW<country_id INT, country_name VARCHAR, country_name_cn VARCHAR, currency VARCHAR, region_name VARCHAR>
  ,f2 ROW<record_timestamp3 TIMESTAMP(3), record_timestamp9 TIMESTAMP(3), time3 TIME(3), time9 TIME(3), gdp DECIMAL(38,18)>) WITH (
    'connector' = 'hbase-1.4',
    'table-name' = 'country',
    'zookeeper.quorum' = 'localhost:2182',
    'zookeeper.znode.parent' = '/hbase' );

CREATE TABLE gmv (
  rowkey VARCHAR,
  f1 ROW<log_ts VARCHAR,item VARCHAR,country_name VARCHAR>
) WITH (
    'connector' = 'hbase-1.4',
    'table-name' = 'gmv',
    'zookeeper.quorum' = 'localhost:2182',
    'zookeeper.znode.parent' = '/hbase',
    'sink.buffer-flush.max-size' = '10mb',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '2s' );

 insert into gmv
 select  rowkey, ROW(max(ts), max(item), max(country_name)) as f1
 from (select concat(cast(o.ts as VARCHAR), '_', item, '_', co.f1.country_name) as rowkey,
 cast(o.ts as VARCHAR) as ts, o.item as item, co.f1.country_name as country_name
 from orders as o
 left outer join currency FOR SYSTEM_TIME AS OF o.proc_time c
 on o.currency = c.currency_name
--  see FLINK-18072
--  left outer join country FOR SYSTEM_TIME AS OF o.proc_time co
--  on c.country = co.rowkey
) a group by rowkey

-- result in hbase:
--  2020-06-08 18:12:53.061_Apple_\xE4\xBA\ column=f1:item, timestamp=1591611172859, value=Apple
--  xBA\xE6\xB0\x91\xE5\xB8\x81
--  2020-06-08 18:12:53.061_Apple_\xE4\xBA\ column=f1:log_ts, timestamp=1591611172859, value=2020-06-08 18:12:53.061
--  xBA\xE6\xB0\x91\xE5\xB8\x81
-- 52 row(s) in 0.0280 seconds
--
-- hbase(main):026:0> scan 'gmv'
