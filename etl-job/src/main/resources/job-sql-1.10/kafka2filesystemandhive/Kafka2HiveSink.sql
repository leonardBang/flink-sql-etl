CREATE TABLE csvData (
  user_name STRING,
  is_new    BOOLEAN,
  content STRING,
  date_col STRING) WITH (
  'connector.type' = 'kafka',
  'connector.version' = '0.10',
  'connector.topic' = 'csv_data',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.group.id' = 'testGroup-1',
  'connector.startup-mode' = 'earliest-offset',
  'format.type' = 'csv')

-- read from kafka, and then write to hive

insert into myhive.hive_test.user_info_kafka select user_name, is_new, content from csvData