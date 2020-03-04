# start kafka cluster
cd /Users/bang/kafka_2.11-0.10.2.0
./bin/zookeeper-server-start.sh -daemon ./config/zookeeper.properties
./bin/kafka-server-start.sh -daemon ./config/server.properties

## relate command
cd ~/confluent-3.2.0/
./bin/kafka-avro-console-producer --broker-list localhost:9092 --topic t1   --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
cd kafka_2.11-0.10.2.0
./bin/kafka-topics.sh --list --zookeeper localhost:2181
./bin/kafka-console-consumer.sh --topic csv_data --bootstrap-server localhost:9092 --from-beginning

# start hdfs
cd /Users/bang/hadoop-2.8.5
hadoop namenode -format
cd /Users/bang/hadoop-2.8.5/sbin
./start-dfs.sh
./start-yarn.sh

# start mysql
cd /usr/local/opt/mysql/bin/mysqld

# start es
cd /Users/bang/elasticsearch-6.3.1
./bin/elasticsearch
## es cli：
./bin/elasticsearch-sql-cli


# start hbase
cd /Users/bang/hbase-1.4.3
./bin/start-hbase.sh
## use own zookeeper wich client port is setted 2182 to avoid conficts with Kafka zookeeper
## : ./bin/hbase shell
## list; create 't1','f1';

# start hive
## start haoop first
./start-dfs.sh
./start-yarn.sh
## start mysql(metastore)
cd /Users/bang/hive-3.1.2
## init hive schema
bin/schematool -initSchema -dbType mysql
bin/hive


