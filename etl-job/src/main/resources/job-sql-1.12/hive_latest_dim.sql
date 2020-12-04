--  build local hive environment
-- $:git clone git@github.com:big-data-europe/docker-hive.git
-- $:cd docker-hive
-- $:docker-compose up -d

-- create hive partition table
create table user_info_latest(user_name string, is_new boolean, content string)
  PARTITIONED BY (date_col STRING) TBLPROPERTIES (
   -- using default partition-name order to load the latest partition every 12h (the most recommended and convenient way)
   'streaming-source.enable' = 'true',
   'streaming-source.partition.include' = 'latest',
   'streaming-source.monitor-interval' = '10 s',
   'streaming-source.partition-order' = 'partition-name'
   );

-- create kafka fact table
CREATE TABLE kafkaTable (
   user_name STRING,
   is_new    BOOLEAN,
   content STRING,
   date_col STRING,proctime as PROCTIME()) WITH (
   'connector' = 'kafka',
   'topic' = 'kafka_user',
   'properties.zookeeper.connect' = 'localhost:2181',
   'properties.bootstrap.servers' = 'localhost:9092',
   'properties.group.id' = 'testCsv',
   'scan.startup.mode' = 'earliest-offset',
   'format' = 'csv');

create table test_csv( user_name VARCHAR, is_new BOOLEAN, content VARCHAR, date_col VARCHAR) with (
 'connector.type' = 'filesystem',
 'connector.path' = '/opt/user_part.csv',
 'format.type' = 'csv')

-- join the latest hive partition
select * from kafkaTable LEFT JOIN user_info_latest
  for system_time as of kafkaTable.proctime as h
  on kafkaTable.user_name = h.user_name;
