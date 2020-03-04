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
   'connector.username' = 'root',
   'connector.table' = 'currency',
   'connector.driver' = 'com.mysql.jdbc.Driver',
   'connector.lookup.cache.max-rows' = '500',
   'connector.lookup.cache.ttl' = '10s',
   'connector.lookup.max-retries' = '3')
CREATE TABLE gmv (
  log_per_min STRING,
  item STRING,
  order_cnt BIGINT,
  currency_time TIMESTAMP(3),
  gmv DECIMAL(38, 18),  timestamp9 TIMESTAMP(3),
  time9 TIME(3),
  gdp  DECIMAL(38, 18)
) WITH (
   'connector.type' = 'jdbc',
   'connector.url' = 'jdbc:mysql://localhost:3306/test',
   'connector.username' = 'root',
   'connector.table' = 'gmv',
   'connector.driver' = 'com.mysql.jdbc.Driver',
   'connector.write.flush.max-rows' = '5000',
   'connector.write.flush.interval' = '2s',
   'connector.write.max-retries' = '3')
insert into gmv
select max(log_ts),
 item, COUNT(order_id) as order_cnt, max(currency_time), cast(sum(amount_kg) * max(rate) as DOUBLE)  as gmv,
 max(timestamp9), max(time9), max(gdp)
 from (
 select cast(o.ts as VARCHAR) as log_ts, o.item as item, o.order_id as order_id, c.currency_time as currency_time,
 o.amount_kg as amount_kg, c.rate as rate, c.timestamp9 as timestamp9, c.time9 as time9, c.gdp as gdp
 from orders as o
 join currency FOR SYSTEM_TIME AS OF o.proc_time c
 on o.currency = c.currency_name
 ) a group by item
