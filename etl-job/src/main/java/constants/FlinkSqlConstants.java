package constants;

public class FlinkSqlConstants {
    public static String ordersTableDDL = "CREATE TABLE orders (\n" +
            "  order_id STRING,\n" +
            "  item    STRING,\n" +
            "  currency STRING,\n" +
            "  amount INT,\n" +
            "  order_time TIMESTAMP(3),\n" +
            "  proc_time as PROCTIME(),\n" +
            "  amount_kg as amount * 1000,\n" +
            "  ts as order_time + INTERVAL '1' SECOND,\n" +
            "  WATERMARK FOR order_time AS order_time\n" +
            ") WITH (\n" +
            "  'connector.type' = 'kafka',\n" +
            "  'connector.version' = '0.10',\n" +
            "  'connector.topic' = 'flink_orders2',\n" +
            "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
            "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
            "  'connector.properties.group.id' = 'testGroup3',\n" +
            "  'connector.startup-mode' = 'earliest-offset',\n" +
            "  'format.type' = 'json',\n" +
            "  'format.derive-schema' = 'true'\n" +
            ")\n";

    public static String ordersTableDDLWithFunc = "CREATE TABLE orders (\n" +
            "  order_id STRING,\n" +
            "  item    STRING,\n" +
            "  currency STRING,\n" +
            "  amount DECIMAL(38, 18),\n" +
            "  order_time TIMESTAMP(3),\n" +
            "  proc_time as PROCTIME(),\n" +
            "  amount_kg as amount * 1000,\n" +
            "  amount_kg_func_value as func(amount),\n" +
            "  ts as order_time + INTERVAL '1' SECOND,\n" +
            "  WATERMARK FOR ts AS ts\n" +
            ") WITH (\n" +
            "  'connector.type' = 'kafka',\n" +
            "  'connector.version' = '0.10',\n" +
            "  'connector.topic' = 'orders_tz',\n" +
            "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
            "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
            "  'connector.properties.group.id' = 'testGroup1',\n" +
            "  'connector.startup-mode' = 'earliest-offset',\n" +
            "  'format.type' = 'json',\n" +
            "  'format.derive-schema' = 'true'\n" +
            ")\n";

    public static final String currencyTableDDL = "CREATE TABLE currency (\n" +
            "  country STRING,\n" +
            "  currency STRING,\n" +
            "  rate INT,\n" +
            "  currency_time TIMESTAMP(3)\n" +
            ") WITH (\n" +
            "  'connector.type' = 'kafka',\n" +
            "  'connector.version' = '0.10',\n" +
            "  'connector.topic' = 'flink_currency',\n" +
            "  'update-mode' = 'append',\n" +
            "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
            "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
            "  'connector.properties.group.id' = 'testGroup',\n" +
            "  'connector.startup-mode' = 'earliest-offset',\n" +
            "  'format.type' = 'json',\n" +
            "  'format.derive-schema' = 'true'\n" +
            ")";

    public static final String mysqlCurrencyDDL = "CREATE TABLE currency (\n" +
            "  currency_id BIGINT,\n" +
            "  currency_name STRING,\n" +
            "  rate DOUBLE,\n" +
            "  currency_time TIMESTAMP(3),\n" +
            "  country STRING,\n" +
            "  timestamp9 TIMESTAMP(3),\n" +
            "  time9 TIME(3),\n" +
            "  gdp DECIMAL(38, 18)\n" +
            ") WITH (\n" +
            "   'connector.type' = 'jdbc',\n" +
            "   'connector.url' = 'jdbc:mysql://localhost:3306/test',\n" +
            "   'connector.username' = 'root'," +
            "   'connector.table' = 'currency',\n" +
            "   'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
            "   'connector.lookup.cache.max-rows' = '500', \n" +
            "   'connector.lookup.cache.ttl' = '10s',\n" +
            "   'connector.lookup.max-retries' = '3'" +
            ")";
    
    public static final String hbaseCountryDDLWithPrecison = "CREATE TABLE country (\n" +
        "  rowkey VARCHAR,\n" +
        "  f1 ROW<country_id INT, country_name VARCHAR, country_name_cn VARCHAR, currency VARCHAR, region_name VARCHAR>, \n" +
        "  f2 ROW<record_timestamp3 TIMESTAMP(3), record_timestamp9 TIMESTAMP(9), time3 TIME(3), time9 TIME(9), gdp DECIMAL(10,4)>" +
        ") WITH (\n" +
        "    'connector.type' = 'hbase',\n" +
        "    'connector.version' = '1.4.3',\n" +
        "    'connector.table-name' = 'country',\n" +
        "    'connector.zookeeper.quorum' = 'localhost:2182',\n" +
        "    'connector.zookeeper.znode.parent' = '/hbase' " +
        ")";

    public static final String hbaseCountryDDLWithFunction = "CREATE TABLE country (\n" +
            "  rowkey VARCHAR,\n" +
            "  f1 ROW<country_id INT, country_name VARCHAR, country_name_cn VARCHAR, currency VARCHAR, region_name VARCHAR> \n" +
            "  ,f2 ROW<record_timestamp3 TIMESTAMP(3), record_timestamp9 TIMESTAMP(9), time3 TIME(3), time9 TIME(9), gdp DECIMAL(38,18)>" +
            ") WITH (\n" +
            "    'connector.type' = 'hbase',\n" +
            "    'connector.version' = '1.4.3',\n" +
            "    'connector.table-name' = 'country',\n" +
            "    'connector.zookeeper.quorum' = 'localhost:2182',\n" +
            "    'connector.zookeeper.znode.parent' = '/hbase' " +
            ")";

    public static String ordersTableDDL11 = "CREATE TABLE orders (\n" +
        "  order_id STRING,\n" +
        "  item    STRING,\n" +
        "  currency STRING,\n" +
        "  amount INT,\n" +
        "  order_time TIMESTAMP(3),\n" +
        "  proc_time as PROCTIME(),\n" +
        "  amount_kg as amount * 1000,\n" +
        "  ts as order_time + INTERVAL '1' SECOND,\n" +
        "  WATERMARK FOR order_time AS order_time\n" +
        ") WITH (\n" +
        "  'connector' = 'kafka-0.10',\n" +
        "  'topic' = 'flink_orders2',\n" +
        "  'properties.zookeeper.connect' = 'localhost:2181',\n" +
        "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
        "  'properties.group.id' = 'testGroup3',\n" +
        "  'scan.startup.mode' = 'earliest-offset',\n" +
        "  'format' = 'json'\n" +
        ")\n";
    public static final String mysqlCurrencyDDL11 = "CREATE TABLE currency (\n" +
        "  currency_id BIGINT,\n" +
        "  currency_name STRING,\n" +
        "  rate DOUBLE,\n" +
        "  currency_time TIMESTAMP(3),\n" +
        "  country STRING,\n" +
        "  timestamp9 TIMESTAMP(3),\n" +
        "  time9 TIME(3),\n" +
        "  gdp DECIMAL(38, 18)\n" +
        ") WITH (\n" +
        "   'connector' = 'jdbc',\n" +
        "   'url' = 'jdbc:mysql://localhost:3306/test',\n" +
        "   'table-name' = 'currency',\n" +
        "   'username' = 'root',\n" +
        "   'password' = '',\n" +
        "   'driver' = 'com.mysql.jdbc.Driver',\n" +
        "   'lookup.cache.max-rows' = '500', \n" +
        "   'lookup.cache.ttl' = '10s',\n" +
        "   'lookup.max-retries' = '3'" +
        ")";
    public static final String hbaseCountryDDLWithPrecison11 = "CREATE TABLE country (\n" +
        "  rowkey VARCHAR,\n" +
        "  f1 ROW<country_id INT, country_name VARCHAR, country_name_cn VARCHAR, currency VARCHAR, region_name VARCHAR>, \n" +
        "  f2 ROW<record_timestamp3 TIMESTAMP(3), record_timestamp9 TIMESTAMP(3), time3 TIME(3), time9 TIME(9), gdp DECIMAL(10,4)>" +
        ") WITH (\n" +
        "    'connector' = 'hbase-1.4',\n" +
        "    'table-name' = 'country',\n" +
        "    'zookeeper.quorum' = 'localhost:2182',\n" +
        "    'zookeeper.znode-parent' = '/hbase' " +
        ")";

}
