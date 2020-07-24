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
    'password' = '',
    'table-name' = 'currency',
    'driver' = 'com.mysql.jdbc.Driver',
    'lookup.cache.max-rows' = '500',
    'lookup.cache.ttl' = '3s',
    'lookup.max-retries' = '3');

select o.order_id, o.item, c.currency_name, c.rate from orders as o
 join currency FOR SYSTEM_TIME AS OF o.proc_time c
 on o.currency = c.currency_name;
