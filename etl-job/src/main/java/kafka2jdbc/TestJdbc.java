package kafka2jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TestJdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);
        String mysqlCurrencyDDL = "CREATE TABLE currency (\n" +
                "  currency_id BIGINT,\n" +
                "  currency_name STRING,\n" +
                "  rate DOUBLE,\n" +
                "  currency_time TIMESTAMP(3),\n" +
                "  country STRING,\n" +
                "  timestamp9 TIMESTAMP(3),\n" +
                "  time9 TIME(3),\n" +
                "  gdp DECIMAL(38, 18)\n" +
                ") WITH (\n" +
                "   'connector.type' = 'jdbc',\n" +
                "   'connector.url' = 'jdbc:mysql://localhost:3306/test',\n" +
                "   'connector.username' = 'root'," +
                "   'connector.table' = 'currency',\n" +
                "   'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
                "   'connector.lookup.cache.max-rows' = '500', \n" +
                "   'connector.lookup.cache.ttl' = '10s',\n" +
                "   'connector.lookup.max-retries' = '3'" +
                ")";

        tableEnvironment.sqlUpdate(mysqlCurrencyDDL);


        String querySQL = "select * from currency" ;

        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery(querySQL), Row.class).print();

        tableEnvironment.execute("KafkaJoinJdbc2Jdbc");
    }
}
