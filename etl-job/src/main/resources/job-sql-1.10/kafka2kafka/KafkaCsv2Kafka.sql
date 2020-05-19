-- from csv data to kafka
create table csv( user_name VARCHAR, is_new BOOLEAN, content VARCHAR) with ( 'connector.type' = 'filesystem',
 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user.csv',
 'format.type' = 'csv')
CREATE TABLE csvData (
  user_name STRING,
  is_new    BOOLEAN,
  content STRING) WITH (
  'connector.type' = 'kafka',
  'connector.version' = '0.10',
  'connector.topic' = 'csv_data',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.group.id' = 'testGroup3',
  'connector.startup-mode' = 'earliest-offset',
  'format.type' = 'csv')
insert into csvData
select user_name, is_new, content from
csv

-- from kafka to csv
CREATE TABLE csvData (
  user_name STRING,
  is_new    BOOLEAN,
  content STRING) WITH (
  'connector.type' = 'kafka',
  'connector.version' = '0.10',
  'connector.topic' = 'csv_data',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.group.id' = 'testGroup4',
  'connector.startup-mode' = 'earliest-offset',
  'format.type' = 'csv')
create table csvTest( user_name VARCHAR, is_new BOOLEAN, content VARCHAR) with ( 'connector.type' = 'filesystem',
 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/test.csv',
 'format.type' = 'csv',
 'update-mode' = 'append',
 'format.fields.0.name' = 'user_name',
 'format.fields.0.data-type' = 'STRING',
 'format.fields.1.name' = 'is_new',
 'format.fields.1.data-type' = 'BOOLEAN',
 'format.fields.2.name' = 'content',
 'format.fields.2.data-type' = 'STRING')
insert into csvTest select user_name, is_new, content from csvData

