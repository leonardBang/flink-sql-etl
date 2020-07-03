package kafka2kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import constants.FlinkSqlConstants;

import java.math.BigDecimal;

public class KafkaJoinKafka2Kafka {
    public static String ordersTableDDL = "CREATE TABLE orders (\n" +
        "  order_id STRING,\n" +
        "  item    STRING,\n" +
        "  currency STRING,\n" +
        "  amount INT,\n" +
        "  order_time TIMESTAMP(3),\n" +
        "  proc_time as PROCTIME()" +
//        ", WATERMARK FOR order_time AS order_time\n" +
        ") WITH (\n" +
        "  'connector' = 'kafka-0.10',\n" +
        "  'topic' = 'flink_orders',\n" +
        "  'properties.zookeeper.connect' = 'localhost:2181',\n" +
        "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
        "  'properties.group.id' = 'testGroup',\n" +
        "  'scan.startup.mode' = 'earliest-offset',\n" +
        "  'format' = 'json'\n" +
        ")\n";

    public static final String currencyTableDDL = "CREATE TABLE currency (\n" +
        "  country STRING,\n" +
        "  currency STRING,\n" +
        "  rate INT,\n" +
        "  rowtime TIMESTAMP(3)" +
//        ",WATERMARK FOR currency_time AS currency_time\n" +
        ") WITH (\n" +
        "  'connector' = 'kafka-0.10',\n" +
        "  'topic' = 'flink_currency',\n" +
        "  'properties.zookeeper.connect' = 'localhost:2181',\n" +
        "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
        "  'properties.group.id' = 'testGroup',\n" +
        "  'scan.startup.mode' = 'earliest-offset',\n" +
        "  'format' = 'json'\n" +
        ")";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);

        tableEnvironment.executeSql(ordersTableDDL);
        tableEnvironment.executeSql(currencyTableDDL);

        String querySQL =
                " select *  \n" +
                " from orders as o \n" +
                " join currency c\n" +
                " on o.currency = c.currency\n";
        String querySQL2 = "SELECT *\n" +
            "FROM currency AS r\n" +
            "WHERE r.rowtime = (\n" +
            "  SELECT MAX(rowtime)\n" +
            "  FROM currency AS r2\n" +
            "  WHERE r2.currency = r.currency\n" +
            "  AND r2.rowtime <=  '10:58:00')";

        System.out.println(tableEnvironment.sqlQuery(querySQL2).explain());
        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery(querySQL2), Row.class).print();
        env.execute();
    }

}
