package usercase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;

public class testUserIssue6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);

        tableEnvironment.connect(
            new Kafka()
                .topic("test-json")
                .version("0.10")
                .property("bootstrap.servers", "localhost:9092")
                .property("zookeeper.connect", "localhost:2181")
                .property("group.id", "testGroup"))
            .withFormat(new Json())
            .withSchema(new Schema()
                .field("general",STRING())
                 .field("data", ROW(
                    FIELD("reference_id", STRING()),
                    FIELD("transaction_type", INT()),
                    FIELD("merchant_id", INT()),
                    FIELD("status", INT()),
                    FIELD("create_time", INT())
                    )
                )
            )
            .createTemporaryTable("KafkaSource");
            tableEnvironment.toAppendStream(tableEnvironment.sqlQuery("select general, reference_id, data.reference_id from KafkaSource"), Row.class)
            .print();
           tableEnvironment.execute("case6");
    }
}
