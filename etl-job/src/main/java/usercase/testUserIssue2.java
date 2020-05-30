package usercase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class testUserIssue2 {
    private static String  kafkaOrdersDDL =  "CREATE TABLE user_log (\n" +
        "  order_id STRING,\n" +
        "  item    STRING,\n" +
        "  currency STRING,\n" +
        "  amount INT,\n" +
        "  order_time TIMESTAMP(3),\n" +
        "  rowtime as order_time,\n" +
        "  amount_kg as amount * 1000,\n" +
        "  WATERMARK FOR rowtime AS rowtime\n" +
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

    private static String mysqlSinkDDL = "CREATE TABLE test_mysql_2 (\n" +
        "vid string,\n" +
        "rss BIGINT,\n" +
        "start_time string\n" +
        ") with ( \n" +
        "   'connector.type' = 'jdbc',\n" +
        "   'connector.url' = 'jdbc:mysql://localhost:3306/test',\n" +
        "   'connector.username' = 'root'," +
        "   'connector.table' = 'task_flink_table_3',\n" +
        "   'connector.write.flush.max-rows' = '100'\n" +
        ")";

    private static String query = "INSERT INTO test_mysql_2\n" +
        " SELECT order_id,rss, start_time FROM(" +
        " SELECT order_id,rss, start_time FROM (\n" +
        "  SELECT  order_id,rss, start_time,\n" +
        "    ROW_NUMBER() OVER (PARTITION BY start_time ORDER BY rss desc) AS rownum\n" +
        "  FROM (\n" +
        "   SELECT order_id,\n" +
        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '5' MINUTE),'yyyy-MM-dd HH:00') AS start_time,\n" +
        "SUM(amount) AS rss\n" +
        "FROM user_log\n" +
        "GROUP BY order_id, TUMBLE(rowtime, INTERVAL '5' MINUTE)\n" +
        "  )\n" +
        ")\n" +
        "WHERE rownum <= 10"
    +") group by order_id,rss, start_time\n";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);

        System.out.println(query);
        tableEnvironment.sqlUpdate(kafkaOrdersDDL);
        tableEnvironment.sqlUpdate(mysqlSinkDDL);
        tableEnvironment.sqlUpdate(query);
//
//        //check the plan
//        System.out.println(tableEnvironment.explain(tableEnvironment.sqlQuery(query)));

//        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery(query), Row.class).print();
        tableEnvironment.execute("reproduce_user_issue");
    }
}
