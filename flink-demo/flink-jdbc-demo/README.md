# Flink JDBC DEMO

*JDBC Catalog* *JDBC CDC format*  *JDBC Dialect*


## 1. Flink standalone 环境准备（基于 Flink 1.11.1 版本）

(a) 下载 flink 安装包
* Flink 安装包: https://mirror.bit.edu.cn/apache/flink/flink-1.11.1/flink-1.11.1-bin-scala_2.11.tgz

 (b) 下载 JDBC connector 相关 jar
* JDBC connector jar: https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc_2.11/1.11.1/flink-connector-jdbc_2.11-1.11.1.jar
* MySQL driver jar： https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/5.1.48/mysql-connector-java-5.1.48.jar
* PostgreSQL driver jar: https://jdbc.postgresql.org/download/postgresql-42.2.14.jar
 
 (c) 下载 Kafka connector 相关 jar
* Kafka connector jar: https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.11.1/flink-sql-connector-kafka_2.11-1.11.1.jar

## 2. 测试数据准备

(a) kafka中的 changelog 数据是通过 debezium connector 抓取的 MySQL orders表 的binlog
     
      (1) 拉起 docker 容器：
            docker-compose -f docker-compose-flink-demo.yaml up 
      (2) 拉起 debezium connector, 开始抓取changelog并吐到kafka: 
            curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-mysql.json 
      (3) 查看 Kafka 中的changelog：
            docker-compose -f docker-compose-flink-demo.yaml exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --from-beginning --topic dbserver1.inventory.orders
       
(b) 维表数据包含一张 customers 表和一张 products 表。
      
      (1)查看维表中数据：
            docker-compose -f docker-compose-flink-demo.yaml exec mysql bash -c 'mysql -u $MYSQL_USER -p$MYSQL_PASSWORD inventory'
 

## 3. 通过 SQL Client 编写 SQL作业 
    ./bin/start-cluster.sh
    ./bin/sql-client.sh embedded -j flink-demo-udf.jar
（a） 创建 Flink 表, 创建 Function
``` 
        -- 订单表：
        CREATE TABLE orders (
           order_number INT NOT NULL,
           order_date INT NOT NULL,
           purchaser INT NOT NULL,
           quantity INT NOT NULL,
           product_id INT NOT NULL,
           proc_time as PROCTIME()
        ) WITH (
           'connector' = 'kafka',
           'topic' = 'dbserver1.inventory.orders',
           'properties.bootstrap.servers' = 'localhost:9092',
           'scan.startup.mode' = 'earliest-offset',
           'format' = 'debezium-json',
           'debezium-json.schema-include' = 'true'
        );
        -- 维表，用户表：
        CREATE TABLE `customers` (
          `id` int NOT NULL,
          `first_name` varchar(255) NOT NULL,
          `last_name` varchar(255) NOT NULL,
          `email` varchar(255) NOT NULL
        ) with (
          'connector' = 'jdbc',
          'url' = 'jdbc:mysql://localhost:3306/inventory',
          'driver' = 'com.mysql.jdbc.Driver',
          'username' = 'mysqluser',
          'password' = 'mysqlpw',
          'table-name' = 'customers',
          'lookup.cache.max-rows' = '100',
          'lookup.cache.ttl' = '10s',
          'lookup.max-retries' = '3'
        );
        
        -- 维表，产品表
        CREATE TABLE `products` (
          `id` int NOT NULL,
          `name` varchar(255)NOT NULL,
          `description` varchar(512) NOT NULL,
          `weight` float NOT NULL
        ) with (
          'connector' = 'jdbc',
          'driver' = 'com.mysql.jdbc.Driver',
          'url' = 'jdbc:mysql://localhost:3306/inventory',
          'username' = 'mysqluser',
          'password' = 'mysqlpw',
          'table-name' = 'products',
          'lookup.cache.max-rows' = '100',
          'lookup.cache.ttl' = '10s',
          'lookup.max-retries' = '3'
         );
        
        -- UDF，把 INT(day from epoch) 转为Date
        create function int2Date as 'udf.Int2DateUDF';
```
        
 (b) 结果表从 pg catalog 获取，不用创建
```
    CREATE CATALOG mypg WITH (
        'type'='jdbc',
        'property-version'='1',
        'base-url'='jdbc:postgresql://localhost:5432/',
        'default-database'='postgres',
        'username'='postgres',
        'password'='postgres'
    );
```

```
     docker-compose -f docker-compose-flink-demo.yaml exec postgres env PGOPTIONS="--search_path=inventory" bash -c 'psql -U $POSTGRES_USER postgres'
     create table enrich_orders(
        order_number integer PRIMARY KEY,
        order_date date NOT NULL,
        purchaser_name varchar(255) NOT NULL,
        purchaser_email varchar(255) NOT NULL,
        quantity integer NOT NULL,
        product_name varchar(255) NOT NULL,
        product_weight DECIMAL(10, 4) NOT NULL,
        total_weight DECIMAL(10, 4) NOT NULL
     );
```
(d) 提交作业到集群
 ```
     insert into mypg.postgres.`inventory.enrich_orders`
     select
        order_number,
        int2Date(order_date),
        concat(c.first_name, ' ', c.last_name) as purchaser_name,
        c.email,
        quantity,
        p.name,
        p.weight,
        p.weight * quantity as total_weight
     from orders as o
     join customers FOR SYSTEM_TIME AS OF o.proc_time c on o.purchaser = c.id
     join products FOR SYSTEM_TIME AS OF o.proc_time p on o.product_id = p.id;
```
(e) 查看 enrich_orders 表已经有 etl 后的数据
     
     docker-compose -f docker-compose-flink-demo.yaml exec postgres env PGOPTIONS="--search_path=inventory" bash -c 'psql -U $POSTGRES_USER postgres'
    
## 4. 测试 CDC 数据同步 和 维表join
 
(a) 新增订单 & 修改订单 & 删除订单
      
      (1) MySQL 原始表 orders
          
          docker-compose -f docker-compose-flink-demo.yaml exec mysql bash -c 'mysql -u $MYSQL_USER -p$MYSQL_PASSWORD inventory'
         
          insert into orders (order_date, purchaser, quantity, product_id) values ('2020-07-30', 1001, 5, 108);
          update orders set quantity = 50 where order_number = 10005;
          update orders set quantity = quantity - 1;
          update orders set quantity = quantity + 11;
          delete from orders where order_number < 10002;
      
      (2) PostgreSQL 结果表 enrich_orders：
         
        docker-compose -f docker-compose-flink-demo.yaml exec postgres env PGOPTIONS="--search_path=inventory" bash -c 'psql -U $POSTGRES_USER postgres'
      
(b) 维表数据更新 
      
      （1） 更新用户的邮件地址
       update customers set email = 'user@flink.apache.org' where id = 1001;
      （2） 用户再次下单：
       insert into orders (order_date, purchaser, quantity, product_id) values ('2020-07-30', 1001, 5, 106);
      