CREATE TABLE orders (
  order_id STRING,
  item    STRING,
  currency STRING,
  amount DOUBLE,
  order_time TIMESTAMP(3),
  proc_time as PROCTIME(),
  amount_kg as amount * 1000,
  ts as order_time + INTERVAL '1' SECOND,
  WATERMARK FOR order_time AS order_time
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = '0.10',
  'connector.topic' = 'flink_orders',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.group.id' = 'testGroup3',
  'connector.startup-mode' = 'earliest-offset',
  'format.type' = 'json',
  'format.derive-schema' = 'true'
)

CREATE TABLE order_cnt (
  log_per_min TIMESTAMP(3),
  item STRING,
  order_cnt BIGINT,
  total_quality BIGINT
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = '0.10',
  'connector.topic' = 'order_cnt',
  'update-mode' = 'append',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'format.type' = 'json',
  'format.derive-schema' = 'true'
)

insert into order_cnt
select TUMBLE_END(ts, INTERVAL '10' SECOND),
 item, COUNT(order_id) as order_cnt, CAST(sum(amount_kg) as BIGINT) as total_quality
from orders
group by item, TUMBLE(ts, INTERVAL '10' SECOND)

