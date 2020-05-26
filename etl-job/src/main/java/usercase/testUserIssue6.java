package usercase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import static org.apache.flink.table.api.DataTypes.STRING;

public class testUserIssue6 {

    private static String hbaseSourceDDL = "CREATE TABLE country (\n" +
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
                .version("0.11")
                .property("bootstrap.servers", "localhost:9092")
                .property("group.id", "testGroup"))
            .withFormat(new Json())
            .withSchema(new Schema()
                .field("table", STRING())
                .field("database", STRING())
                .field("gmt_time", STRING())
                // process time 声明： descriptor API 生成的 处理时间 列名为proctime
                .proctime()
                // event time 声明：gmt_time为schema中的列, watermark 的delay为5s
                .rowtime(new Rowtime().timestampsFromField("gmt_time").watermarksPeriodicBounded(5 * 1000L))

         )
//            .withSchema(new Schema()
//                .field("data", ROW(
//                    FIELD("reference_id", STRING()),
//                    FIELD("transaction_type", INT()),
//                    FIELD("merchant_id", INT()),
//                    FIELD("status", INT()),
//                    FIELD("create_time", INT())
//                    )
//                ).rowtime(new Rowtime().timestampsFromField("xxx"))
//            )
            .createTemporaryTable("KafkaSource");



        //{
        //   "database":"main_db",
        //   "maxwell_ts":1590416550358000,
        //   "table":"transaction_tab",
        //   "data":{
        //       "transaction_sn":"8888",
        //       "parent_id":0,
        //       "user_id":333,
        //       "amount":555,
        //       "reference_id":"666",
        //       "status":3,
        //       "transaction_type":3,
        //       "merchant_id":2,
        //       "update_time":1590416550,
        //       "create_time":1590416550
        //   }
        //}
    }
}
