package usercase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class testUserIssue4 {

    private static String mysqlTable = "create table tb(id string, cooper bigint, user_sex string) with(\n" +
        "    'connector.type' = 'jdbc',\n" +
        "    'connector.url' = 'jdbc:mysql://localhost:3306/test',\n" +
        "    'connector.username' = 'root',\n" +
        "    'connector.table' = 'tb'\n" +
        ")";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);
        tableEnvironment.sqlUpdate(mysqlTable);
        String querySQL = "select id, cooper from tb";
        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery(querySQL), Row.class).print();

         tableEnvironment.execute("reproduce_user_issue");
    }
}
