we can use flink-sql-client to do all SQL tests.
(1) Add necessary connector jar to flink intall directory's lib, eg: if you want to test read from kafka and write to elasticsearch
please add flink-sql-connector-kafka.jar and flink-sql-connector-elasticsearch.jar to lib.
(2) Set up necessary component like kafka cluster/elasticsearch cluster/mysql/hbase.
(3) start flink cluster, start sql-client.
(4) post related sql to sql-client to test. 