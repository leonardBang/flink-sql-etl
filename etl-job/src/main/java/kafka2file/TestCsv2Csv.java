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
                " id `BIGINT`," +
                " note STRING," +
                " country STRING," +
                " record_time TIMESTAMP(4)," +
                " doub_val DECIMAL(6, 2)" +
                ") with (" +
                " 'connector.type' = 'filesystem',\n" +
                " 'connector.path' = '/Users/bang/sourcecode/project/Improve/flinkstream/src/main/resources/test.csv',\n" +
                " 'format.type' = 'csv',\n" +
                " 'format.fields.0.name' = 'id',\n" +
                " 'format.fields.0.data-type' = 'BIGINT',\n" +
                " 'format.fields.1.name' = 'note',\n" +
                " 'format.fields.1.data-type' = 'STRING',\n" +
                " 'format.fields.2.name' = 'country',\n" +
                " 'format.fields.2.data-type' = 'STRING',\n" +
                " 'format.fields.3.name' = 'record_time',\n" +
                " 'format.fields.3.data-type' = 'TIMESTAMP(4)'," +
                " 'format.fields.4.name' = 'doub_val',\n" +
                " 'format.fields.4.data-type' = 'DECIMAL(6, 2)'" +
                ")";
        tableEnvironment.sqlUpdate(csvSourceDDL);
        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery("select * from csv"), Row.class).print();

        tableEnvironment.execute("csvTest");

    }

}
