create table csv( pageId VARCHAR, eventId VARCHAR, recvTime VARCHAR) with ( 'connector.type' = 'filesystem',
 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user3.csv',
 'format.type' = 'csv',
 'format.fields.0.name' = 'pageId',
 'format.fields.0.data-type' = 'STRING',
 'format.fields.1.name' = 'eventId',
 'format.fields.1.data-type' = 'STRING',
 'format.fields.2.name' = 'recvTime',
 'format.fields.2.data-type' = 'STRING')

CREATE TABLE test_upsert (
  aggId varchar ,
  pageId varchar ,
  ts varchar ,
  expoCnt bigint ,
  clkCnt bigint
) WITH (
'connector.type' = 'elasticsearch',
'connector.version' = '6',
'connector.hosts' = 'http://localhost:9200',
'connector.index' = 'flink_zhangle_pageview',
'connector.document-type' = '_doc',
'update-mode' = 'upsert',
'connector.key-delimiter' = '$',
'connector.key-null-literal' = 'n/a',
'connector.bulk-flush.interval' = '1000',
'format.type' = 'json'
)

INSERT INTO test_upsert
  SELECT aggId, pageId, ts,
  count(case when eventId = 'exposure' then 1 else null end) as expoCnt,
  count(case when eventId = 'click' then 1 else null end) as clkCnt
  FROM
  (
    SELECT
        'ZL_001' as aggId,
        pageId,
        eventId,
        recvTime,
        ts2Date(recvTime) as ts
    from csv
    where eventId in ('exposure', 'click')
  ) as t1
  group by aggId, pageId, ts