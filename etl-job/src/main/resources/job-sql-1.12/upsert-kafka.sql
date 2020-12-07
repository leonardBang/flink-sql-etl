
-- create an upsert-kafka table
 CREATE TABLE pageviews_per_region (
   region STRING,
   pv BIGINT,
   uv BIGINT,
   PRIMARY KEY (region) NOT ENFORCED
 ) WITH (
   'connector' = 'upsert-kafka',
   'topic' = 'pageviews_per_region',
   'properties.bootstrap.servers' = 'localhost:9092',
   'key.format' = 'json',
   'value.format' = 'json'
 );

 -- write test data to  upsert-kafka table
insert into pageviews_per_region values('test1', 100, 20);
insert into pageviews_per_region values('test2', 200, 20);
insert into pageviews_per_region values('test1', 101, 20);


-- check data has been writen into kafka
./bin/kafka-console-consumer.sh --topic pageviews_per_region --bootstrap-server localhost:9092 --from-beginning --property print.key=true --property key.separator="-"
-- {"region":"test1"}-{"region":"test1","pv":100,"uv":20}
-- {"region":"test2"}-{"region":"test2","pv":200,"uv":200}
-- {"region":"test1"}-{"region":"test1","pv":101,"uv":20}

-- Read upsert kafka in sql client, the key {"region":"test1"} should update with the new value
select * from pageviews_per_region;
-- region    pv    uv
--  test2   200   200
--  test1   101    20