# Flink Temporal Join Versioned Table DEMO

*Temporal Join* *Versioned Table*  *mysql-cdc*


## 1. Flink standalone 环境准备（基于 Flink 1.12.2 版本）

(a) 下载 flink 安装包
* Flink 安装包: https://mirrors.tuna.tsinghua.edu.cn/apache/flink/flink-1.12.2/flink-1.12.2-bin-scala_2.11.tgz

(b) 下载 Kafka connector jar
     * Kafka connector jar: https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.12.2/flink-sql-connector-kafka_2.11-1.12.2.jar

(c) 下载 mysql-cdc connector jar
 * MySQL CDC jar： https://repo1.maven.org/maven2/com/alibaba/ververica/flink-sql-connector-mysql-cdc/1.2.0/flink-sql-connector-mysql-cdc-1.2.0.jar
  
(d) 下载 JDBC connector jar
 * JDBC connector jar: https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc_2.11/1.12.2/flink-connector-jdbc_2.11-1.12.2.jar
 * MySQL driver jar： https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/5.1.48/mysql-connector-java-5.1.48.jar
 
## 2. 测试数据准备

(a) 创建 MySQL 业务表 
    拉起 docker 容器：
          docker-compose -f temporal-join-versioned-table.yaml up 
    创建 MySQL 表：
        docker-compose -f temporal-join-versioned-table.yaml exec mysql bash -c 'mysql -u $MYSQL_USER -p$MYSQL_PASSWORD inventory'
``` 
-- 产品表，用作维表，补齐订单流的产品明细信息
CREATE TABLE `demo_products` (
  `product_id` int(11) NOT NULL AUTO_INCREMENT,
  `product_name` varchar(255) NOT NULL,
  `price` float NOT NULL,
  `currency` varchar(100) NOT NULL, 
  PRIMARY KEY (`product_id`) 
) ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8;

insert into demo_products(product_name,price,currency) values 
('Apple', 5.0, 'RMB'),
('Orange', 4.0, 'RMB'),
('Hammer', 100.0, 'RMB');

-- 订单表，存放用户订单
 CREATE TABLE `demo_orders` (
  `order_id` int(11) NOT NULL AUTO_INCREMENT,
  `order_date` date NOT NULL,
  `order_time` TIMESTAMP(3) NOT NULL,
  `quantity` int(11) NOT NULL,
  `product_id` int(11) NOT NULL,
  `purchaser` varchar(512) NOT NULL,
    PRIMARY KEY (`order_id`)
   ) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8;

INSERT INTO demo_orders(order_date, order_time,quantity, product_id, purchaser) values 
('2021-03-08', '2021-03-08 09:00:00.000', 30, 500, 'flink'),
('2021-03-08', '2021-03-08 09:01:00.000', 40, 501, 'flink'),
('2021-03-08', '2021-03-08 09:02:00.000', 50, 502, 'flink');

-- 结果表，存放 ETL 加工后的订单宽表
-- MYSQL-75439: https://bugs.mysql.com/bug.php?id=75439, 两个 TIMESTAMP 列在一张表中定义的问题，我们这里简单地定义成 STRING 绕过
  CREATE TABLE `enrich_orders`(
          `order_id` int(11) NOT NULL,
          `order_date` date NOT NULL,
          `order_time` varchar(128),
           `quantity` int(11) ,
          `purchaser` varchar(512) ,
          `product_id` int(11) ,
          `product_name` varchar(255) ,
          `product_update_time` varchar(128),
           `price` float,
          `currency` varchar(100) ,
          `total_price` DECIMAL(10, 4),
           PRIMARY KEY (`order_id`)
) ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8;
```

（b）监控订单表 demo_orders 的产出, Flink 自带了CDC connector，可以直接捕获表的changelog
 ```
    docker-compose -f temporal-join-versioned-table.yaml exec mysql bash -c 'mysql -u root -p$MYSQL_ROOT_PASSWORD inventory'
    mysql> GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mysqluser' IDENTIFIED BY 'mysqlpw';
    mysql> FLUSH PRIVILEGES;
```

 (c) kafka中维表(产品表)的changelog 数据是通过 debezium connector 抓取的 MySQL demo_products 的binlog, 这里没有直接用mysql-cdc 工具，是为了读取changelog meta信息中的时间字段，
 用来追踪维表的历史版本，订单流不用追踪历史版本，可以直接用 mysql-cdc 工具。
     
      (1) 拉起 debezium connector, 开始抓取 changelog并吐到 kafka: 
            curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-mysql.json 
      (2) 查看 Kafka 中的产品表的 changelog：
            docker-compose -f temporal-join-versioned-table.yaml exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --from-beginning --topic dbserver1.inventory.demo_products
      
 ## 3. 通过 SQL Client 运行 SQL作业 
    ./bin/start-cluster.sh
    ./bin/sql-client.sh embedded 
（a） 创建 Flink 表, 创建 Function
``` 
        -- 订单表：
        CREATE TABLE demoOrders (
         `order_id` INTEGER ,
          `order_date` DATE ,
          `order_time` TIMESTAMP(3),
          `quantity` INT ,
          `product_id` INT ,
          `purchaser` STRING,
           WATERMARK FOR order_time AS order_time 
         ) WITH (
          'connector' = 'mysql-cdc',
          'hostname' = 'localhost',
          'port' = '3306',
          'username' = 'mysqluser',
          'password' = 'mysqlpw',
          'database-name' = 'inventory',
          'table-name' = 'demo_orders');

        -- 维表，产品表
        CREATE TABLE `demoProducts` (
          `product_id` INTEGER,
          `product_name` STRING,
          `price` DECIMAL(10, 4),
          `currency` STRING,
           update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,
          PRIMARY KEY(product_id) NOT ENFORCED,
          WATERMARK FOR update_time AS update_time
        ) WITH (
          'connector' = 'kafka',
           'topic' = 'dbserver1.inventory.demo_products',
           'properties.bootstrap.servers' = 'localhost:9092',
           'scan.startup.mode' = 'earliest-offset',
           'format' = 'debezium-json',
           'debezium-json.schema-include' = 'true');
        
        -- 结果表，存放 ETL 加工后的订单
          CREATE TABLE enrichOrders(
            `order_id` INTEGER,
            `order_date` DATE,
            `order_time` STRING,
            `quantity` INTEGER,
            `purchaser` STRING,
            `product_id` INTEGER,
            `product_name` STRING,
            `product_update_time` STRING,
            `price` DECIMAL(10, 4) ,
            `currency` STRING,
            `total_price` DECIMAL(10, 4),
            PRIMARY KEY(order_id) NOT ENFORCED
            ) WITH (
              'connector' = 'jdbc',
              'url' = 'jdbc:mysql://localhost:3306/inventory',
              'username' = 'mysqluser',
              'password' = 'mysqlpw',
              'table-name' = 'enrich_orders');
```

    
## 4. 测试订单更新和产品更新


(a) 新增订单 & 修改订单 
    
    (1) 新增订单：
    docker-compose -f temporal-join-versioned-table.yaml exec mysql bash -c 'mysql -u $MYSQL_USER -p$MYSQL_PASSWORD inventory'
    
    mysql> INSERT INTO demo_orders(order_date, order_time,quantity, product_id, purchaser) values 
    ('2021-03-08', '2021-03-08 09:00:00.000', 30, 501, 'flink');
    
    Flink SQL> 
               SELECT o.order_id, o.order_date, o.order_time, o.quantity, o.purchaser, p.product_id,
                   p.product_name, p.update_time, p.price, p.currency, p.price * o.quantity as total_price  
               FROM demoOrders as o 
               LEFT JOIN demoProducts FOR SYSTEM_TIME AS OF o.order_time p 
               ON o.product_id = p.product_id;
    
    
    (2) 修改订单：
    mysql> UPDATE demo_orders set quantity = 100 where order_id = 1001;
    
    Flink SQL> 
               SELECT o.order_id, o.order_date, o.order_time, o.quantity, o.purchaser, p.product_id,
                   p.product_name, p.update_time, p.price, p.currency, p.price * o.quantity as total_price  
               FROM demoOrders as o 
               LEFT JOIN demoProducts FOR SYSTEM_TIME AS OF o.order_time p 
               ON o.product_id = p.product_id;


(b) 关联不同版本的维表
  
       mysql> insert into demo_products(product_name,price,currency) values ('Scooter', 99.99, 'RMB'); 
       
       Flink SQL> select * from demoProducts;
       
       mysql> INSERT INTO demo_orders(order_date, order_time,quantity, product_id, purchaser) values  ('2021-03-09', '2021-03-09 18:00:16', 30, 503, 'flink');
       mysql> INSERT INTO demo_orders(order_date, order_time,quantity, product_id, purchaser) values  ('2021-03-09', '2021-03-09 20:10:30', 30, 503, 'flink');
     
      
(c) 提交作业到 Flink 集群，写入 MySQL 结果表
     
     Flink SQL> 
             INSERT INTO `enrichOrders`
             SELECT o.order_id, o.order_date, CAST(o.order_time AS STRING), o.quantity, o.purchaser, p.product_id,
                 p.product_name, cast(p.update_time AS STRING), p.price, p.currency, p.price * o.quantity as total_price  
             FROM demoOrders as o 
             LEFT JOIN demoProducts FOR SYSTEM_TIME AS OF o.order_time p 
             ON o.product_id = p.product_id;
        
     
      查看 enrich_orders 表已经有 etl 后的数据
      mysql> SELECT * FROM enrich_orders;

