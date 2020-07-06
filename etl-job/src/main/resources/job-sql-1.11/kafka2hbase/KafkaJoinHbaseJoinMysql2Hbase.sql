CREATE TABLE orders (
  order_id STRING,
  item    STRING,
  currency STRING,
  amount INT,
  order_time TIMESTAMP(3),
  proc_time as PROCTIME(),
  amount_kg as amount * 1000,
  WATERMARK FOR order_time AS order_time
) WITH (
  'connector' = 'kafka',
  'topic' = 'flink_orders3',
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
  f1 ROW<log_ts VARCHAR,item VARCHAR,country_name VARCHAR,country_name_cn VARCHAR,region_name VARCHAR,
   currency VARCHAR,order_cnt BIGINT,currency_time TIMESTAMP(3), gmv DOUBLE>
) WITH (
    'connector' = 'hbase-1.4',
    'table-name' = 'gmv',
    'zookeeper.quorum' = 'localhost:2182',
    'zookeeper.znode.parent' = '/hbase',
    'sink.buffer-flush.max-size' = '10mb',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '2s' );

insert into gmv  select concat(log_ts,'_',item) as rowkey,
 ROW(log_ts, item, country_name, country_name_cn, region_name, currency, order_cnt, currency_time, gmv) as f1
 from  (select  co.f1.country_name as country_name, co.f1.country_name_cn as country_name_cn, co.f1.region_name as region_name, co.f1.currency as currency, cast(TUMBLE_END(o.ts, INTERVAL '10' SECOND) as VARCHAR) as log_ts,
 o.item, COUNT(o.order_id) as order_cnt, c.currency_time, cast(sum(o.amount_kg) * c.rate as DOUBLE)  as gmv
 from orders as o
 left outer join currency FOR SYSTEM_TIME AS OF o.proc_time c
 on o.currency = c.currency_name
 -- see FLINK-18072
--  left outer join country FOR SYSTEM_TIME AS OF o.proc_time co
--  on c.country = co.rowkey
 group by o.item, c.currency_time, c.rate, co.f1.country_name, co.f1.country_name_cn, co.f1.region_name, co.f1.currency, TUMBLE(o.ts, INTERVAL '10' SECOND)) a

insert into gmv  select concat(log_ts,'_',item) as rowkey,
 ROW(log_ts, item, country_name, country_name_cn, region_name, currency, order_cnt, currency_time, gmv) as f1 from  (
 select  'test' as country_name, 'test' as country_name_cn,'test' as region_name, 'test' as currency, cast(TUMBLE_END(o.order_time, INTERVAL '10' SECOND) as VARCHAR) as log_ts,
 o.item, COUNT(o.order_id) as order_cnt, c.currency_time, cast(sum(o.amount_kg) * c.rate as DOUBLE)  as gmv
 from orders as o
 left outer join currency FOR SYSTEM_TIME AS OF o.proc_time c
 on o.currency = c.currency_name
 group by o.item, c.currency_time, c.rate, 'test', 'test', 'test', 'test', TUMBLE(o.order_time, INTERVAL '10' SECOND)) a


-- result in hbase
-- 2020-06-08 18:13:00.000_\xE9\x85\xB8\xE column=f1:region_name, timestamp=1591630452428, value=test
--  5\xA5\xB6
-- 233 row(s) in 0.2560 seconds