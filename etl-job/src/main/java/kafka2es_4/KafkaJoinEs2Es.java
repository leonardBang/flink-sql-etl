package kafka2es_4;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import constants.FlinkSqlConstants;

public class KafkaJoinEs2Es {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);

//        tableEnvironment.sqlUpdate(FlinkSqlConstants.ordersTableDDL);
        tableEnvironment.sqlUpdate(FlinkSqlConstants.esCurrencyDDL);
        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery("select * from currency"), Row.class).print();

//        String sinkTableDDL =  "CREATE TABLE gmv (\n" +
//                "  log_per_min STRING,\n" +
//                "  item STRING,\n" +
//                "  order_cnt BIGINT,\n" +
//                "  currency_time TIMESTAMP(2),\n" +
//                "  gmv DECIMAL(38, 4)," +
//                "  timestamp9 TIMESTAMP(6),\n" +
//                "  time9 TIME(6),\n" +
//                "  gdp DECIMAL(10, 4)\n" +
//                ") WITH (\n" +
//                "   'connector.type' = 'jdbc',\n" +
//                "   'connector.url' = 'jdbc:mysql://localhost:3306/test',\n" +
//                "   'connector.username' = 'root'," +
//                "   'connector.table' = 'gmv',\n" +
//                "   'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
//                "   'connector.write.flush.max-rows' = '5000', \n" +
//                "   'connector.write.flush.interval' = '2s', \n" +
//                "   'connector.write.max-retries' = '3'" +
//                ")";
//        tableEnvironment.sqlUpdate(sinkTableDDL);
//
//        String querySQL = "insert into gmv \n" +
//                "select cast(TUMBLE_END(o.ts, INTERVAL '10' SECOND) as VARCHAR) as log_ts,\n" +
//                " o.item, COUNT(o.order_id) as order_cnt, c.currency_time, cast(sum(o.amount_kg) * c.rate as DECIMAL(38, 4))  as gmv,\n" +
//                " c.timestamp9, c.time9, c.gdp\n" +
//                "from orders as o \n" +
//                "join currency FOR SYSTEM_TIME AS OF o.proc_time c\n" +
//                "on o.currency = c.currency_name\n" +
//                "group by o.item, c.currency_time, c.rate, c.timestamp9, c.time9, c.gdp, TUMBLE(o.ts, INTERVAL '10' SECOND)\n" ;
//
//        tableEnvironment.sqlUpdate(querySQL);

        tableEnvironment.execute("KafkaJoinJdbc2Jdbc");
    }

    public static class AddOneFunc extends ScalarFunction {
        public Long eval(long t) {
             return t + 1;
        }

        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return Types.LONG;
        }
    }
}
