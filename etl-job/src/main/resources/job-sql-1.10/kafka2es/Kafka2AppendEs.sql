## batch
create table csv( pageId VARCHAR, eventId VARCHAR, recvTime VARCHAR) with ( 'connector.type' = 'filesystem',
 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user3.csv',
 'format.type' = 'csv',
 'format.fields.0.name' = 'pageId',
 'format.fields.0.data-type' = 'STRING',
 'format.fields.1.name' = 'eventId',
 'format.fields.1.data-type' = 'STRING',
 'format.fields.2.name' = 'recvTime',
 'format.fields.2.data-type' = 'STRING')

CREATE TABLE es_table (
  aggId varchar ,
  pageId varchar ,
  ts varchar ,
  expoCnt int ,
  clkCnt int
) WITH (
'connector.type' = 'elasticsearch',
'connector.version' = '7',
'connector.hosts' = 'http://localhost:9200',
'connector.index' = 'cli_test',
'connector.document-type' = '_doc',
'update-mode' = 'upsert',
'connector.key-delimiter' = '$',
'connector.key-null-literal' = 'n/a',
'connector.bulk-flush.interval' = '1000',
'format.type' = 'json'
)
INSERT INTO es_table
  SELECT  pageId,eventId,cast(recvTime as varchar) as ts, 1, 1 from csv


 ## streaming

 create table csv_user( user_name VARCHAR, is_new BOOLEAN, content VARCHAR) with ( 'connector.type' = 'filesystem',
 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user.csv',
 'format.type' = 'csv',
 'format.fields.0.name' = 'user_name',
 'format.fields.0.data-type' = 'STRING',
 'format.fields.1.name' = 'is_new',
 'format.fields.1.data-type' = 'BOOLEAN',
 'format.fields.2.name' = 'content',
 'format.fields.2.data-type' = 'STRING')
CREATE TABLE kafka_user (
  user_name STRING,
  is_new    BOOLEAN,
  content STRING) WITH (
  'connector.type' = 'kafka',
  'connector.version' = '0.10',
  'connector.topic' = 'kafka_user',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.group.id' = 'testGroup3',
  'connector.startup-mode' = 'earliest-offset',
  'format.type' = 'csv')
insert into kafka_user
select user_name, is_new, content from
csv_user;

CREATE TABLE es_user (
  user_name STRING,
  is_new    BOOLEAN,
  content STRING
) WITH (
'connector.type' = 'elasticsearch',
'connector.version' = '7',
'connector.hosts' = 'http://localhost:9200',
'connector.index' = 'es_user',
'connector.document-type' = '_doc',
'update-mode' = 'upsert',
'connector.key-delimiter' = '$',
'connector.key-null-literal' = 'n/a',
'connector.bulk-flush.interval' = '1000',
'format.type' = 'json'
);
insert into es_user
select user_name, is_new, content from
kafka_user;

