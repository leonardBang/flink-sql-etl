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
  'connector.topic' = 'flink_orders3',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.group.id' = 'testGroup4',
  'connector.startup-mode' = 'earliest-offset',
  'format.type' = 'json',
  'format.derive-schema' = 'true'
)

CREATE TABLE country (
  rowkey VARCHAR,
  f1 ROW<country_id INT, country_name VARCHAR, country_name_cn VARCHAR, currency VARCHAR, region_name VARCHAR>
 ) WITH (
    'connector.type' = 'hbase',
    'connector.version' = '1.4.3',
    'connector.table-name' = 'country',
    'connector.zookeeper.quorum' = 'localhost:2182',
    'connector.zookeeper.znode.parent' = '/hbase' )

CREATE TABLE currency (
  currency_id BIGINT,
  currency_name STRING,
  rate DOUBLE,
  currency_time TIMESTAMP(3),
  country STRING,
  timestamp9 TIMESTAMP(3),
  time9 TIME(3),
  gdp DOUBLE
) WITH (
   'connector.type' = 'jdbc',
   'connector.url' = 'jdbc:mysql://localhost:3306/test',
   'connector.username' = 'root',   'connector.table' = 'currency',
   'connector.driver' = 'com.mysql.jdbc.Driver',
   'connector.lookup.cache.max-rows' = '500',
   'connector.lookup.cache.ttl' = '10s',
   'connector.lookup.max-retries' = '3')


CREATE TABLE gmv (
  rowkey VARCHAR,
  f1 ROW<log_ts VARCHAR,item VARCHAR,country_name VARCHAR,country_name_cn VARCHAR,region_name VARCHAR,
   currency VARCHAR,order_cnt BIGINT,currency_time TIMESTAMP(3), gmv DOUBLE>
) WITH (
    'connector.type' = 'hbase',
    'connector.version' = '1.4.3',
    'connector.table-name' = 'gmv1',
    'connector.zookeeper.quorum' = 'localhost:2182',
    'connector.zookeeper.znode.parent' = '/hbase',
    'connector.write.buffer-flush.max-size' = '10mb',
    'connector.write.buffer-flush.max-rows' = '1000',
    'connector.write.buffer-flush.interval' = '2s' )


insert into gmv  select concat(log_ts,'_',item) as rowkey,
 ROW(log_ts, item, country_name, country_name_cn, region_name, currency, order_cnt, currency_time, gmv) as f1 from  (select  co.f1.country_name as country_name, co.f1.country_name_cn as country_name_cn, co.f1.region_name as region_name, co.f1.currency as currency, cast(TUMBLE_END(o.ts, INTERVAL '10' SECOND) as VARCHAR) as log_ts,
 o.item, COUNT(o.order_id) as order_cnt, c.currency_time, cast(sum(o.amount_kg) * c.rate as DOUBLE)  as gmv
 from orders as o
 left outer join currency FOR SYSTEM_TIME AS OF o.proc_time c
 on o.currency = c.currency_name
 left outer join country FOR SYSTEM_TIME AS OF o.proc_time co
 on c.country = co.rowkey group by o.item, c.currency_time, c.rate, co.f1.country_name, co.f1.country_name_cn, co.f1.region_name, co.f1.currency, TUMBLE(o.ts, INTERVAL '10' SECOND)) a