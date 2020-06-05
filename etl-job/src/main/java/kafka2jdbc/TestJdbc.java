package kafka2jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
                "  timestamp9 TIMESTAMP(6),\n" +
                "  time9 TIME(3),\n" +
                "  gdp DECIMAL(10, 6)\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/test',\n" +
                "   'username' = 'root'," +
                "   'password' = ''," +
                "   'table-name' = 'currency',\n" +
                "   'driver' = 'com.mysql.jdbc.Driver',\n" +
                "   'lookup.cache.max-rows' = '500', \n" +
                "   'lookup.cache.ttl' = '10s',\n" +
                "   'lookup.max-retries' = '3'" +
                ")";
        System.out.println(mysqlCurrencyDDL);

        tableEnvironment.sqlUpdate(mysqlCurrencyDDL);


        String querySQL = "select * from currency" ;

        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery(querySQL), Row.class).print();
        env.execute();
//        tableEnvironment.execute("KafkaJoinJdbc2Jdbc");
    }
}
