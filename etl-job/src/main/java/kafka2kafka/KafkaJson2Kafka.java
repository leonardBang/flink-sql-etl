package kafka2kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class KafkaJson2Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);

        String sourceTableDDL = "CREATE TABLE orders (\n" +
                "  order_id STRING,\n" +
                "  item    STRING,\n" +
                "  currency STRING,\n" +
                "  amount DOUBLE,\n" +
                "  order_time TIMESTAMP(3),\n" +
                "  proc_time as PROCTIME(),\n" +
                "  amount_kg as amount * 1000,\n" +
                "  ts as order_time + INTERVAL '1' SECOND,\n" +
                "  WATERMARK FOR order_time AS order_time" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = '0.10',\n" +
                "  'connector.topic' = 'flink_orders',\n" +
                "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'connector.properties.group.id' = 'testGroup3',\n" +
                "  'connector.startup-mode' = 'earliest-offset',\n" +
                "  'format.type' = 'json',\n" +
                "  'format.derive-schema' = 'true'\n" +
                ")\n";
        tableEnvironment.sqlUpdate(sourceTableDDL);

        String sinkTableDDL = "CREATE TABLE order_cnt (\n" +
                "  log_per_min TIMESTAMP(3),\n" +
                "  item STRING,\n" +
                "  order_cnt BIGINT,\n" +
                "  total_quality BIGINT\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = '0.10',\n" +
                "  'connector.topic' = 'order_cnt',\n" +
                "  'update-mode' = 'append',\n" +
                "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'format.type' = 'json',\n" +
                "  'format.derive-schema' = 'true'\n" +
                ")";
        tableEnvironment.sqlUpdate(sinkTableDDL);

        String querySQL = "insert into order_cnt \n" +
                "select TUMBLE_END(order_time, INTERVAL '10' SECOND),\n" +
                " item, COUNT(order_id) as order_cnt, CAST(sum(amount_kg) as BIGINT) as total_quality\n" +
                "from orders\n" +
                "group by item, TUMBLE(order_time, INTERVAL '10' SECOND)\n" ;

        tableEnvironment.sqlUpdate(querySQL);
        System.out.println(sourceTableDDL);
        System.out.println(sinkTableDDL);
        System.out.println(querySQL);

        tableEnvironment.execute("StreamKafka2KafkaJob");
    }
}
