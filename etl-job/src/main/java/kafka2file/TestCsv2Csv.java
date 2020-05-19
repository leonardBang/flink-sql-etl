package kafka2file;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TestCsv2Csv {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment, environmentSettings);

        String csvSourceDDL = "create table csv(" +
                " id INT," +
                " note STRING," +
                " country STRING," +
                " record_time TIMESTAMP(4)," +
                " doub_val DECIMAL(6, 2)" +
                ") with (" +
                " 'connector.type' = 'filesystem',\n" +
                " 'connector.path' = '/Users/bang/sourcecode/project/Improve/flinkstream/src/main/resources/test.csv',\n" +
                " 'format.type' = 'csv'" +
                ")";
        String csvSink = "create table csvSink(" +
                " jnlno STRING,\n" +
                "  taskid char(4),\n" +
                "   hit VARCHAR " +
                ") with (" +
                " 'connector.type' = 'filesystem',\n" +
                " 'connector.path' = '/Users/bang/sourcecode/project/Improve/flinkstream/src/main/resources/test12312.csv',\n" +
                " 'format.type' = 'csv'" +
                ")";
        tableEnvironment.sqlUpdate(csvSourceDDL);
        tableEnvironment.sqlUpdate(csvSink);
        tableEnvironment.sqlUpdate("insert into  csvSink select a.country,'111111qeq','false' from csv a");
        System.out.println(csvSourceDDL);
        System.out.println(csvSink);
        System.out.println("insert into  csvSink select a.country,'111111qeq','false' from csv a");

        //
//        tableEnvironment.toAppendStream(
//                tableEnvironment.sqlQuery("insert into  target select a.country,'111111qeq','false' from csv a"),
//                Row.class).print();
        tableEnvironment.execute("csvTest");

    }

}
