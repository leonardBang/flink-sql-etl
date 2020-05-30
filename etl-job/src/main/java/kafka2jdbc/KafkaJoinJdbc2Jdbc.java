package kafka2jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class KafkaJoinJdbc2Jdbc {
    private static String kafkaSourceDDL = "CREATE TABLE orders (\n" +
            "  order_id STRING,\n" +
            "  item    STRING,\n" +
            "  currency STRING,\n" +
            "  amount INT,\n" +
            "  order_time TIMESTAMP(3),\n" +
            "  proc_time as PROCTIME(),\n" +
            "  amount_kg as amount * 1000,\n" +
            "  WATERMARK FOR order_time AS order_time\n" +
            ") WITH (\n" +
            "  'connector.type' = 'kafka',\n" +
            "  'connector.version' = '0.10',\n" +
            "  'connector.topic' = 'flink_orders3',\n" +
            "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
            "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
            "  'connector.properties.group.id' = 'test-jdbc',\n" +
            "  'connector.startup-mode' = 'earliest-offset',\n" +
            "  'format.type' = 'json',\n" +
            "  'format.derive-schema' = 'true'\n" +
            ")\n";

    private static String mysqlDimDDL = "CREATE TABLE currency (\n" +
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
            ")";;
    private static  String mysqlSinkTableDDL =  "CREATE TABLE gmv (\n" +
            "  log_per_min STRING,\n" +
            "  item STRING,\n" +
            "  order_cnt BIGINT,\n" +
            "  currency_time TIMESTAMP(3),\n" +
            "  gmv DECIMAL(38, 18)," +
            "  timestamp9 TIMESTAMP(3),\n" +
            "  time9 TIME(3),\n" +
            "  gdp DECIMAL(38, 18)\n" +
            ") WITH (\n" +
            "   'connector.type' = 'jdbc',\n" +
            "   'connector.url' = 'jdbc:mysql://localhost:3306/test',\n" +
            "   'connector.username' = 'root'," +
            "   'connector.table' = 'gmv_table',\n" +
            "   'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
            "   'connector.write.flush.max-rows' = '3', \n" +
            "   'connector.write.flush.interval' = '120s', \n" +
            "   'connector.write.max-retries' = '2'" +
            ")";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);


        tableEnvironment.sqlUpdate(kafkaSourceDDL);
        tableEnvironment.sqlUpdate(mysqlDimDDL);
        tableEnvironment.sqlUpdate(mysqlSinkTableDDL);

        String querySQL = "insert into gmv \n" +
                "select cast(TUMBLE_END(o.order_time, INTERVAL '10' SECOND) as VARCHAR) as log_ts,\n" +
                " o.item, COUNT(o.order_id) as order_cnt, c.currency_time, cast(sum(o.amount_kg) * c.rate as DECIMAL(38, 18))  as gmv,\n" +
                " c.timestamp9, c.time9, c.gdp\n" +
                "from orders as o \n" +
                "join currency FOR SYSTEM_TIME AS OF o.proc_time c\n" +
                "on o.currency = c.currency_name\n" +
                "group by o.item, c.currency_time, c.rate, c.timestamp9, c.time9, c.gdp, TUMBLE(o.order_time, INTERVAL '10' SECOND)\n" ;

        System.out.println(kafkaSourceDDL);
        System.out.println(mysqlDimDDL);
        System.out.println(mysqlSinkTableDDL);
        System.out.println(querySQL);
        tableEnvironment.sqlUpdate(querySQL);
//        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery(querySQL), Row.class).print();
        tableEnvironment.execute("KafkaJoinJdbc2Jdbc");
    }

}
