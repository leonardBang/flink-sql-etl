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
   'url' = 'jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8',
   'username' = 'root',
    'password' = '',
   'table-name' = 'currency',
   'driver' = 'com.mysql.jdbc.Driver')
