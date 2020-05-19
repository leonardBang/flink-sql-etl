-- first job: build avro format data from csv and write to kafka topic
create table csv( user_name VARCHAR, is_new BOOLEAN, content VARCHAR) with ( 'connector.type' = 'filesystem',
 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user.csv',
 'format.type' = 'csv')
CREATE TABLE AvroTest (
  user_name VARCHAR,
  is_new BOOLEAN,
  content VARCHAR) WITH (
  'connector.type' = 'kafka',
  'connector.version' = '0.10',
  'connector.topic' = 'avro_from_csv',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.group.id' = 'testGroup3',
  'connector.startup-mode' = 'earliest-offset',
  'format.type' = 'avro',
  'format.record-class' = 'kafka.UserAvro'
)

insert into AvroTest select user_name, is_new, content from csv

-- second job: consume avro format data from kafka and write to another kafka topic

CREATE TABLE AvroTest (
  user_name VARCHAR,
  is_new BOOLEAN,
  content VARCHAR) WITH (
  'connector.type' = 'kafka',
  'connector.version' = '0.10',
  'connector.topic' = 'avro_from_csv',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.group.id' = 'testGroup4',
  'connector.startup-mode' = 'earliest-offset',
  'format.type' = 'avro',
  'format.record-class' = 'kafka.UserAvro'
)

CREATE TABLE WikipediaFeed_filtered (
  user_name STRING,
  is_new    BOOLEAN,
  content STRING) WITH (
  'connector.type' = 'kafka',
  'connector.version' = '0.10',
  'connector.topic' = 'WikipediaFeed2_filtered',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.group.id' = 'testGroup3',
  'connector.startup-mode' = 'earliest-offset',
  'format.type' = 'avro',
  'format.avro-schema' =
    '{
    "type": "record",
    "name": "UserAvro",
    "fields": [
      {"name": "user_name", "type": "string"},
      {"name": "is_new", "type": "boolean"},
      {"name": "content", "type": "string"}
      ]
    }')

insert into WikipediaFeed_filtered
select user_name, is_new, content
from AvroTest