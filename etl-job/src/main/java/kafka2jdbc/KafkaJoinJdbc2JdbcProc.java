package kafka2jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import constants.FlinkSqlConstants;

public class KafkaJoinJdbc2JdbcProc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);

        tableEnvironment.sqlUpdate(FlinkSqlConstants.ordersTableDDL);
        tableEnvironment.sqlUpdate(FlinkSqlConstants.mysqlCurrencyDDL);

        String sinkTableDDL =  "CREATE TABLE gmv (\n" +
                "  log_per_min STRING,\n" +
                "  item STRING,\n" +
                "  order_cnt BIGINT,\n" +
                "  currency_time TIMESTAMP(3),\n" +
                "  gmv DECIMAL(38, 18)," +
                "  timestamp9 TIMESTAMP(3),\n" +
                "  time9 TIME(3),\n" +
                "  gdp DECIMAL(38, 18)\n" +
                ") WITH (\n" +
                "   'connector.type' = 'jdbc',\n" +
                "   'connector.url' = 'jdbc:mysql://localhost:3306/test',\n" +
                "   'connector.username' = 'root'," +
                "   'connector.table' = 'gmv',\n" +
                "   'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
                "   'connector.write.flush.max-rows' = '5000', \n" +
                "   'connector.write.flush.interval' = '2s', \n" +
                "   'connector.write.max-retries' = '3'" +
                ")";
        tableEnvironment.sqlUpdate(sinkTableDDL);

        String querySQL = "insert into gmv \n" +
                "select cast(TUMBLE_END(o.proc_time, INTERVAL '10' SECOND) as VARCHAR) as log_ts,\n" +
                " o.item, COUNT(o.order_id) as order_cnt, c.currency_time, cast(sum(o.amount_kg) * c.rate as DECIMAL(38, 4))  as gmv,\n" +
                " c.timestamp9, c.time9, c.gdp\n" +
                "from orders as o \n" +
                "join currency FOR SYSTEM_TIME AS OF o.proc_time c\n" +
                "on o.currency = c.currency_name\n" +
                "group by o.item, c.currency_time, c.rate, c.timestamp9, c.time9, c.gdp, TUMBLE(o.proc_time, INTERVAL '10' SECOND)\n" ;
        tableEnvironment.sqlUpdate(querySQL);

        tableEnvironment.execute("KafkaJoinJdbc2Jdbc");
    }

}
