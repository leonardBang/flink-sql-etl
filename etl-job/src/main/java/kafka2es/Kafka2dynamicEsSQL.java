package kafka2es;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import org.apache.derby.impl.sql.catalog.SYSCOLUMNSRowFactory;

import java.sql.Timestamp;

public class Kafka2dynamicEsSQL {
    private static String csvSourceDDL = "create table csv(" +
            " pageId VARCHAR," +
            " eventId VARCHAR," +
            " recvTime TIMESTAMP(3)" +
            ") with (" +
            " 'connector.type' = 'filesystem',\n" +
            " 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user3.csv',\n" +
            " 'format.type' = 'csv',\n" +
            " 'format.fields.0.name' = 'pageId',\n" +
            " 'format.fields.0.data-type' = 'STRING',\n" +
            " 'format.fields.1.name' = 'eventId',\n" +
            " 'format.fields.1.data-type' = 'STRING',\n" +
            " 'format.fields.2.name' = 'recvTime',\n" +
            " 'format.fields.2.data-type' = 'TIMESTAMP(3)')";
    private static String sinkDDL = "CREATE TABLE append_test (\n" +
            "  aggId varchar ,\n" +
            "  pageId varchar ,\n" +
            "  ts timestamp(3) ,\n" +
            "  expoCnt int ,\n" +
            "  clkCnt int\n" +
            ") WITH (\n" +
            "'connector.type' = 'elasticsearch',\n" +
            "'connector.version' = '6',\n" +
            "'connector.hosts' = 'http://localhost:9200',\n" +
            "'connector.index' = 'dadynamic-index-{clkCnt}',\n" +
            "'connector.document-type' = '_doc',\n" +
            "'update-mode' = 'upsert',\n" +
            "'connector.key-delimiter' = '$',\n" +
            "'connector.key-null-literal' = 'n/a',\n" +
            "'connector.bulk-flush.interval' = '1000',\n" +
            "'format.type' = 'json'\n" +
            ")\n";
    private static String query = "INSERT INTO append_test\n" +
            "  SELECT  pageId,eventId,recvTime ts, 1, 1 from csv";
    private static String mysqlSinkDDL = "CREATE TABLE nonExisted (\n" +
            "  c0 BOOLEAN," +
            "  c1 INTEGER," +
            "  c2 BIGINT," +
            "  c3 FLOAT," +
            "  c4 DOUBLE," +
            "  c5 DECIMAL(38, 18)," +
            "  c6 STRING," +
            "  c7 DATE," +
            "  c8 TIME(0)," +
            "  c9 TIMESTAMP(3)" +
            ") WITH (\n" +
            "   'connector.type' = 'jdbc',\n" +
            "   'connector.url' = 'jdbc:mysql://localhost:3306/test',\n" +
            "   'connector.username' = 'root'," +
            "   'connector.table' = 'nonExisted',\n" +
            "   'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
            "   'connector.write.auto-create-table' = 'true' " +
            ")";
    private static String queryMysql = "insert into nonExisted " +
            "select true as c0, id, cast(id as bigint),cast(doub_val as float),cast(doub_val as double), doub_val,country," +
            " date_val, time_val, record_time from csv";
    public static void main(String[] args) throws Exception {
        // legacy planner test passed
//         testLegacyPlanner();

        // blink planner test passed
        testBlinkPlanner();
    }

    public static void testLegacyPlanner() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
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
        System.out.println(csvSourceDDL);
        System.out.println(sinkDDL);
        System.out.println(query);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);
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
