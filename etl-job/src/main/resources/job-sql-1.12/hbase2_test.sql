// prepare cdc data
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic product_binlog
cat ~/sourcecode/project/flink-1.11/flink/flink-formats/flink-json/src/test/resources/debezium-data-schema-exclude.txt | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic product_binlog
bin/kafka-console-consumer.sh --topic product_binlog --bootstrap-server localhost:9092 --from-beginning

// test write to hbase
CREATE TABLE product_binlog1 (
  id INT NOT NULL,
  name STRING,
  description STRING,
  weight DECIMAL(10,3)
 ) WITH (
  'connector' = 'kafka',
  'topic' = 'product_binlog1',
  'properties.bootstrap.servers' = 'localhost:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
 );

 CREATE TABLE hbase_product (
  id INT NOT NULL PRIMARY KEY NOT ENFORCED,
  f1 ROW<name STRING, description STRING>
 ) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'product1',
    'zookeeper.quorum' = 'localhost:2182',
    'zookeeper.znode.parent' = '/hbase',
    'sink.buffer-flush.max-size' = '10mb',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '2s' );

insert into hbase_product select id, ROW(name,description) from product_binlog1;