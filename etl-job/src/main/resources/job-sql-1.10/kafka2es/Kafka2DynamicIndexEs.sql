create table csv( pageId VARCHAR, eventId VARCHAR, recvTime TIMESTAMP(3)) with ( 'connector.type' = 'filesystem',
 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user3.csv',
 'format.type' = 'csv',
 'format.fields.0.name' = 'pageId',
 'format.fields.0.data-type' = 'STRING',
 'format.fields.1.name' = 'eventId',
 'format.fields.1.data-type' = 'STRING',
 'format.fields.2.name' = 'recvTime',
 'format.fields.2.data-type' = 'TIMESTAMP(3)')

CREATE TABLE append_test (
  aggId varchar ,
  pageId varchar ,
  ts timestamp(3) ,
  expoCnt int ,
  clkCnt int
) WITH (
'connector.type' = 'elasticsearch',
'connector.version' = '6',
'connector.hosts' = 'http://localhost:9200',
'connector.index' = 'dadynamic-index-{clkCnt}',
'connector.document-type' = '_doc',
'update-mode' = 'upsert',
'connector.key-delimiter' = '$',
'connector.key-null-literal' = 'n/a',
'connector.bulk-flush.interval' = '1000',
'format.type' = 'json'
)

INSERT INTO append_test
  SELECT  pageId,eventId,recvTime ts, 1, 1 from csv
