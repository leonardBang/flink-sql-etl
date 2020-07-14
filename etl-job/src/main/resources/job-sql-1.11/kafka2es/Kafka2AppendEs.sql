## batch
create table csv( pageId VARCHAR, eventId VARCHAR, recvTime VARCHAR) with ( 'connector' = 'filesystem',
 'path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user3.csv',
 'format' = 'csv')

CREATE TABLE es_table (
  aggId varchar ,
  pageId varchar ,
  ts varchar ,
  expoCnt int ,
  clkCnt int
) WITH (
'connector' = 'elasticsearch-6',
'hosts' = 'http://localhost:9200',
'index' = 'usercase13',
'document-type' = '_doc',
'document-id.key-delimiter' = '$',
'sink.bulk-flush.interval' = '1000',
'format' = 'json'
)
INSERT INTO es_table
  SELECT  pageId,eventId,cast(recvTime as varchar) as ts, 1, 1 from csv


 ## streaming

 create table csv_user( user_name VARCHAR, is_new BOOLEAN, content VARCHAR) with ( 'type' = 'filesystem',
 'path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user.csv',
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
  'type' = 'kafka',
  'version' = '0.10',
  'topic' = 'kafka_user',
  'properties.zookeeper.connect' = 'localhost:2181',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup3',
  'startup-mode' = 'earliest-offset',
  'format.type' = 'csv')
insert into kafka_user
select user_name, is_new, content from
csv_user;

CREATE TABLE es_user (
  user_name STRING,
  is_new    BOOLEAN,
  content STRING
) WITH (
'type' = 'elasticsearch',
'version' = '7',
'hosts' = 'http://localhost:9200',
'index' = 'es_user',
'document-type' = '_doc',
'update-mode' = 'upsert',
'key-delimiter' = '$',
'key-null-literal' = 'n/a',
'bulk-flush.interval' = '1000',
'format.type' = 'json'
);
insert into es_user
select user_name, is_new, content from
kafka_user;

