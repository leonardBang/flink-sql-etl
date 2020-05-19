package kafka2hbase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import constants.FlinkSqlConstants;

public class KafkaJoinHbaseJoinMysql2Hbase {
    private static String  kafkaOrdersDDL =  "CREATE TABLE orders (\n" +
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
            "  'connector.topic' = 'flink_orders3',\n" +
            "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
            "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
            "  'connector.properties.group.id' = 'testGroup4',\n" +
            "  'connector.startup-mode' = 'earliest-offset',\n" +
            "  'format.type' = 'json',\n" +
            "  'format.derive-schema' = 'true'\n" +
            ")\n";
    private static String hbaseDimTableDDL =   "CREATE TABLE country (\n" +
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
    private static String mysqlDimTableDDL =   "CREATE TABLE currency (\n" +
            "  currency_id BIGINT,\n" +
            "  currency_name STRING,\n" +
            "  rate DOUBLE,\n" +
            "  currency_time TIMESTAMP(3),\n" +
            "  country STRING,\n" +
            "  timestamp9 TIMESTAMP(3),\n" +
            "  time9 TIME(3),\n" +
            "  gdp DOUBLE\n" +
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

    private static String hbaseSinkTableDDL = "CREATE TABLE gmv (\n" +
            "  rowkey VARCHAR,\n" +
            "  f1 ROW<log_ts VARCHAR,item VARCHAR,country_name VARCHAR,country_name_cn VARCHAR,region_name VARCHAR,\n" +
            "   currency VARCHAR,order_cnt BIGINT,currency_time TIMESTAMP(3), gmv DOUBLE> \n" +
            ") WITH (\n" +
            "    'connector.type' = 'hbase',\n" +
            "    'connector.version' = '1.4.3',\n" +
            "    'connector.table-name' = 'gmv1',\n" +
            "    'connector.zookeeper.quorum' = 'localhost:2182',\n" +
            "    'connector.zookeeper.znode.parent' = '/hbase',\n" +
            "    'connector.write.buffer-flush.max-size' = '10mb', \n" +
            "    'connector.write.buffer-flush.max-rows' = '1000',  \n" +
            "    'connector.write.buffer-flush.interval' = '2s' " +
            ")";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);

        tableEnvironment.sqlUpdate(kafkaOrdersDDL);
        tableEnvironment.sqlUpdate(hbaseDimTableDDL);
        tableEnvironment.sqlUpdate(mysqlDimTableDDL);
        tableEnvironment.sqlUpdate(hbaseSinkTableDDL);

        String querySQL = "insert into gmv  select concat(log_ts,'_',item) as rowkey,\n" +
                " ROW(log_ts, item, country_name, country_name_cn, region_name, currency, order_cnt, currency_time, gmv) as f1 from " +
                " (select  co.f1.country_name as country_name, co.f1.country_name_cn as country_name_cn," +
                " co.f1.region_name as region_name, co.f1.currency as currency," +
                " cast(TUMBLE_END(o.ts, INTERVAL '10' SECOND) as VARCHAR) as log_ts,\n" +
                " o.item, COUNT(o.order_id) as order_cnt, c.currency_time, cast(sum(o.amount_kg) * c.rate as DOUBLE)  as gmv\n" +
                " from orders as o \n" +
                " left outer join currency FOR SYSTEM_TIME AS OF o.proc_time c\n" +
                " on o.currency = c.currency_name\n" +
                " left outer join country FOR SYSTEM_TIME AS OF o.proc_time co\n" +
                " on c.country = co.rowkey" +
                " group by o.item, c.currency_time, c.rate, co.f1.country_name, co.f1.country_name_cn, co.f1.region_name, co.f1.currency," +
                " TUMBLE(o.ts, INTERVAL '10' SECOND)) a\n" ;
        System.out.println(kafkaOrdersDDL);
        System.out.println(hbaseDimTableDDL);
        System.out.println(mysqlDimTableDDL);
        System.out.println(hbaseSinkTableDDL);
        System.out.println(querySQL);

//        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery(querySQL), Row.class).print();
        tableEnvironment.sqlUpdate(querySQL);

        tableEnvironment.execute("hbase2hbase_ETL_job");


    }
}
