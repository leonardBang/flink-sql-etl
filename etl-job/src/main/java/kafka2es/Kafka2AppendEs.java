package kafka2es;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class Kafka2AppendEs {
    private static String csvSourceDDL = "create table csv(" +
            " pageId VARCHAR," +
            " eventId VARCHAR," +
            " recvTime VARCHAR" +
            ") with (" +
            " 'connector.type' = 'filesystem',\n" +
            " 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user3.csv',\n" +
            " 'format.type' = 'csv',\n" +
            " 'format.fields.0.name' = 'pageId',\n" +
            " 'format.fields.0.data-type' = 'STRING',\n" +
            " 'format.fields.1.name' = 'eventId',\n" +
            " 'format.fields.1.data-type' = 'STRING',\n" +
            " 'format.fields.2.name' = 'recvTime',\n" +
            " 'format.fields.2.data-type' = 'STRING')";
    private static String sinkDDL = "CREATE TABLE append_test (\n" +
            "  aggId varchar ,\n" +
            "  pageId varchar ,\n" +
            "  ts varchar ,\n" +
            "  expoCnt int ,\n" +
            "  clkCnt int\n" +
            ") WITH (\n" +
            "'connector.type' = 'elasticsearch',\n" +
            "'connector.version' = '7',\n" +
            "'connector.hosts' = 'http://localhost:9200',\n" +
            "'connector.index' = 'append_test7',\n" +
            "'connector.document-type' = '_doc',\n" +
            "'update-mode' = 'upsert',\n" +
            "'connector.key-delimiter' = '$',\n" +
            "'connector.key-null-literal' = 'n/a',\n" +
            "'connector.bulk-flush.interval' = '1000',\n" +
            "'format.type' = 'json'\n" +
            ")\n";
    private static String query = "INSERT INTO append_test\n" +
            "  SELECT  pageId,eventId,ts2Date(recvTime) as ts, 1, 1 from csv";


    public static void main(String[] args) throws Exception {
        System.out.println(csvSourceDDL);
        System.out.print(sinkDDL);
        System.out.print(query);

        // legacy planner test passed
//         testLegacyPlanner();

        // blink planner test passed
        testBlinkPlanner();
//        System.out.println(sinkDDL);
    }

    public static void testLegacyPlanner() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);
        tableEnvironment.registerFunction("ts2Date", new ts2Date());

        tableEnvironment.sqlUpdate(csvSourceDDL);
        tableEnvironment.sqlUpdate(sinkDDL);
        tableEnvironment.sqlUpdate(query);

        tableEnvironment.execute("Kafka2Es");
    }

    public static void testBlinkPlanner() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);
        tableEnvironment.registerFunction("ts2Date", new ts2Date());
        tableEnvironment.sqlUpdate(csvSourceDDL);
        tableEnvironment.sqlUpdate(sinkDDL);
        tableEnvironment.sqlUpdate(query);

        tableEnvironment.execute("Kafka2Es");
    }

    public static class ts2Date extends ScalarFunction {
        public String eval(String timeStr) {
            Timestamp t = Timestamp.valueOf(timeStr);
            return t.getDate() + " " + t.getHours() + "：" + t.getMinutes();
        }

        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return Types.STRING;
        }
    }
}
