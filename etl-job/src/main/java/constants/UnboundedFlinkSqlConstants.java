package constants;

public class UnboundedFlinkSqlConstants {
    public static String ordersTableDDL = "CREATE TABLE orders (\n" +
            "  order_id STRING,\n" +
            "  item    STRING,\n" +
            "  currency STRING,\n" +
            "  amount DECIMAL(10, 4),\n" +
            "  order_time TIMESTAMP(3),\n" +
            "  proc_time as PROCTIME(),\n" +
            "  amount_kg as amount * 1000,\n" +
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

    public static String ordersTableDDLWithFunc = "CREATE TABLE orders (\n" +
            "  order_id STRING,\n" +
            "  item    STRING,\n" +
            "  currency STRING,\n" +
            "  amount DECIMAL(10, 4),\n" +
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
            "  rate DECIMAL(38, 4),\n" +
            "  currency_time TIMESTAMP(3),\n" +
            "  country STRING,\n" +
            "  timestamp9 TIMESTAMP(6),\n" +
            "  currency_next as currency_id + 1,\n" +
            "  time9 TIME(6),\n" +
            "  gdp DECIMAL(10, 4)\n" +
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
    public static final String hbaseCountryDDL= "CREATE TABLE country (\n" +
            "  rowkey VARCHAR,\n" +
            "  f1 ROW<country_id INT, country_name VARCHAR, country_name_cn VARCHAR, currency VARCHAR, region_name VARCHAR> \n" +
            " " +
            ") WITH (\n" +
            "    'connector.type' = 'hbase',\n" +
            "    'connector.version' = '1.4.3',\n" +
            "    'connector.table-name' = 'country',\n" +
            "    'connector.zookeeper.quorum' = 'localhost:2182',\n" +
            "    'connector.zookeeper.znode.parent' = '/hbase' " +
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
            "  expand_id as ExpandIdFunc(f1.country_id)," +
            "  f1 ROW<country_id INT, country_name VARCHAR, country_name_cn VARCHAR, currency VARCHAR, region_name VARCHAR>, \n" +
            "  f2 ROW<record_timestamp3 TIMESTAMP(3), record_timestamp9 TIMESTAMP(9), time3 TIME(3), time9 TIME(9), gdp DECIMAL(10,4)>" +
            ") WITH (\n" +
            "    'connector.type' = 'hbase',\n" +
            "    'connector.version' = '1.4.3',\n" +
            "    'connector.table-name' = 'country',\n" +
            "    'connector.zookeeper.quorum' = 'localhost:2182',\n" +
            "    'connector.zookeeper.znode.parent' = '/hbase' " +
            ")";

}
