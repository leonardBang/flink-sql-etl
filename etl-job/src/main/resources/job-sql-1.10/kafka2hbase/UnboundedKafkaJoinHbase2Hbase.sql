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
  'connector.type' = 'kafka',
  'connector.version' = '0.10',
  'connector.topic' = 'flink_orders2',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.group.id' = 'testGroup3',
  'connector.startup-mode' = 'earliest-offset',
  'format.type' = 'json',
  'format.derive-schema' = 'true'
)

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
   'connector.type' = 'jdbc',
   'connector.url' = 'jdbc:mysql://localhost:3306/test',
   'connector.username' = 'root',   'connector.table' = 'currency',
   'connector.driver' = 'com.mysql.jdbc.Driver',
   'connector.lookup.cache.max-rows' = '500',
   'connector.lookup.cache.ttl' = '10s',
   'connector.lookup.max-retries' = '3')

CREATE TABLE country (
  rowkey VARCHAR,
  f1 ROW<country_id INT, country_name VARCHAR, country_name_cn VARCHAR, currency VARCHAR, region_name VARCHAR>
  ,f2 ROW<record_timestamp3 TIMESTAMP(3), record_timestamp9 TIMESTAMP(9), time3 TIME(3), time9 TIME(9), gdp DECIMAL(38,18)>) WITH (
    'connector.type' = 'hbase',
    'connector.version' = '1.4.3',
    'connector.table-name' = 'country',
    'connector.zookeeper.quorum' = 'localhost:2182',
    'connector.zookeeper.znode.parent' = '/hbase' )

CREATE TABLE gmv (
  rowkey VARCHAR,
  f1 ROW<log_ts VARCHAR,item VARCHAR,country_name VARCHAR>
) WITH (
    'connector.type' = 'hbase',
    'connector.version' = '1.4.3',
    'connector.table-name' = 'gmv',
    'connector.zookeeper.quorum' = 'localhost:2182',
    'connector.zookeeper.znode.parent' = '/hbase',
    'connector.write.buffer-flush.max-size' = '10mb',
    'connector.write.buffer-flush.max-rows' = '1000',
    'connector.write.buffer-flush.interval' = '2s' )

 insert into gmv
 select  rowkey, ROW(max(ts), max(item), max(country_name)) as f1
 from (select concat(cast(o.ts as VARCHAR), '_', item, '_', co.f1.country_name) as rowkey,
 cast(o.ts as VARCHAR) as ts, o.item as item, co.f1.country_name as country_name
 from orders as o
 left outer join currency FOR SYSTEM_TIME AS OF o.proc_time c
 on o.currency = c.currency_name
 left outer join country FOR SYSTEM_TIME AS OF o.proc_time co
 on c.country = co.rowkey
) a group by rowkey

