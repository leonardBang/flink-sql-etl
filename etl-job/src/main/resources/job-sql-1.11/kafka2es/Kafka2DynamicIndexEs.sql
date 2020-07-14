create table csv1( pageId VARCHAR, eventId VARCHAR, recvTime TIMESTAMP(3)) with ( 'connector' = 'filesystem',
 'path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user3.csv',
 'format' = 'csv')

CREATE TABLE append_test (
  aggId varchar ,
  pageId varchar ,
  ts timestamp(3) ,
  expoCnt int ,
  clkCnt int
) WITH (
'connector' = 'elasticsearch-7',
'hosts' = 'http://localhost:9200',
'index' = 'xudynamic-index-{clkCnt}',
'document-id.key-delimiter' = '$',
'sink.bulk-flush.interval' = '1000',
'format' = 'json'
);

INSERT INTO append_test
  SELECT  pageId,eventId,recvTime ts, 1, 1 from csv1;
