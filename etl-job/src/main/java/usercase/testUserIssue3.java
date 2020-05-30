package usercase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class testUserIssue3 {

    private static String kafkaSourceDDL = "create table json_table(" +
        " w_es BIGINT," +
        " w_type STRING," +
        " w_isDdl BOOLEAN," +
        " w_data ARRAY<ROW<pay_info STRING, online_fee DOUBLE, sign STRING, account_pay_fee DOUBLE>>," +
        " w_ts TIMESTAMP(3)," +
        " w_table STRING" +
        ") WITH (\n" +
        "  'connector.type' = 'kafka',\n" +
        "  'connector.version' = '0.10',\n" +
        "  'connector.topic' = 'json-test2',\n" +
        "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
        "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
        "  'connector.properties.group.id' = 'test-jdbc',\n" +
        "  'connector.startup-mode' = 'earliest-offset',\n" +
        "  'format.type' = 'json',\n" +
        "  'format.derive-schema' = 'true'\n" +
        ")\n";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);
        tableEnvironment.sqlUpdate(kafkaSourceDDL);
        String querySQL = "select w_ts," +
            " 'test' as city1_id, " +
            " w_data[1].pay_info,\n" +
            " w_data as pay_order_id from json_table";
        System.out.println(kafkaSourceDDL);
        System.out.println(querySQL);
        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery(querySQL), Row.class).print();

         tableEnvironment.execute("reproduce_user_issue");
    }

    //@Test
    //	public void testArray() throws Exception {
    //		String jsonStr = "{" +
    //			"\"w_es\":1589870637000," +
    //			"\"w_type\":\"INSERT\"," +
    //			"\"w_isDdl\":false," +
    //			"\"w_data\":[" +
    //			"{\"pay_info\":\"channelId=82&onlineFee=89.0&outTradeNo=0&payId=0&payType=02&rechargeId=4&totalFee=89.0&tradeStatus=success&userId=32590183789575&sign=00\"," +
    //			"\"online_fee\":\"89.0\"," +
    //			"\"sign\":\"00\"," +
    //			"\"account_pay_fee\":\"0.0\"}]," +
    //			"\"w_ts\":\"2020-05-20T13:58:37.131Z\"," +
    //			"\"w_table\":\"cccc111\"}";
    //		System.out.println(jsonStr);
    //		DataType rowType = ROW(
    //			FIELD("w_es", DataTypes.BIGINT()),
    //			FIELD("w_type", DataTypes.STRING()),
    //			FIELD("w_isDdl", DataTypes.BOOLEAN()),
    //			FIELD("w_data", ARRAY(ROW(
    //				FIELD("pay_info", DataTypes.STRING()),
    //				FIELD("online_fee", DataTypes.DECIMAL(38, 4)),
    //				FIELD("sign", DataTypes.STRING()),
    //				FIELD("account_pay_fee", DataTypes.DECIMAL(38, 4))
    //			))),
    //			FIELD("w_ts", DataTypes.TIMESTAMP()),
    //			FIELD("w_table", DataTypes.STRING()));
    //		JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema.Builder(
    //			(TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(rowType))
    //			.build();
    //		Row row = deserializationSchema.deserialize(jsonStr.getBytes());
    //		System.out.println(row);
    //	}
}
