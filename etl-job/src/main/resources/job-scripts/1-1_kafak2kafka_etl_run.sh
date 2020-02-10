# start kafka cluster
cd /Users/bang/kafka_2.11-0.10.2.0
./bin/zookeeper-server-start.sh -daemon ./config/zookeeper.properties
./bin/kafka-server-start.sh -daemon ./config/server.properties

# start hdfs
cd /Users/bang/hadoop-2.8.5
hadoop namenode -format
cd /Users/bang/hadoop-2.8.5/sbin
./start-dfs.sh
./start-yarn.sh

# start mysql
cd /usr/local/opt/mysql/bin/mysqld

# start es
cd /Users/bang/elasticsearch-5.1.2
./bin/elasticsearch

# start hbase
cd /Users/bang/hbase-1.4.3
./bin/start-hbase.sh

# start hive
cd /Users/bang/apache-hive-3.1.2-bin


