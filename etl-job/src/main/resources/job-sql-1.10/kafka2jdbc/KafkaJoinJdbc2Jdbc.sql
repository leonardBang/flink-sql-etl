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
  'connector.type' = 'kafka',
  'connector.version' = '0.10',
  'connector.topic' = 'flink_orders3',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.group.id' = 'test-jdbc',
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


CREATE TABLE gmv (
  log_per_min STRING,
  item STRING,
  order_cnt BIGINT,
  currency_time TIMESTAMP(3),
  gmv DECIMAL(38, 18),  timestamp9 TIMESTAMP(3),
  time9 TIME(3),
  gdp DECIMAL(38, 18)
) WITH (
   'connector.type' = 'jdbc',
   'connector.url' = 'jdbc:mysql://localhost:3306/test',
   'connector.username' = 'root',   'connector.table' = 'gmv_table',
   'connector.driver' = 'com.mysql.jdbc.Driver',
   'connector.write.flush.max-rows' = '3',
   'connector.write.flush.interval' = '120s',
   'connector.write.max-retries' = '2')

insert into gmv
select cast(TUMBLE_END(o.order_time, INTERVAL '10' SECOND) as VARCHAR) as log_ts,
 o.item, COUNT(o.order_id) as order_cnt, c.currency_time, cast(sum(o.amount_kg) * c.rate as DECIMAL(38, 18))  as gmv,
 c.timestamp9, c.time9, c.gdp
from orders as o
join currency FOR SYSTEM_TIME AS OF o.proc_time c
on o.currency = c.currency_name
group by o.item, c.currency_time, c.rate, c.timestamp9, c.time9, c.gdp, TUMBLE(o.order_time, INTERVAL '10' SECOND)