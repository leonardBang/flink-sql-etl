package kafka2jdbc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import constants.FlinkSqlConstants;

public class UnboundedKafkaJoinJdbc2Jdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);

        tableEnvironment.registerFunction("add_one_fun", new AddOneFunc());

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
                "  gdp  DECIMAL(38, 18)\n" +
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
                "select max(log_ts),\n" +
                " item, COUNT(order_id) as order_cnt, max(currency_time), cast(sum(amount_kg) * max(rate) as DOUBLE)  as gmv,\n" +
                " max(timestamp9), max(time9), max(gdp) \n" +
                " from ( \n" +
                " select cast(o.ts as VARCHAR) as log_ts, o.item as item, o.order_id as order_id, c.currency_time as currency_time,\n" +
                " o.amount_kg as amount_kg, c.rate as rate, c.timestamp9 as timestamp9, c.time9 as time9, c.gdp as gdp \n" +
                " from orders as o \n" +
                " join currency FOR SYSTEM_TIME AS OF o.proc_time c \n" +
                " on o.currency = c.currency_name \n" +
                " ) a group by item\n" ;

        System.out.println(FlinkSqlConstants.ordersTableDDL);
        System.out.println(FlinkSqlConstants.mysqlCurrencyDDL);
        System.out.println(sinkTableDDL);
        System.out.println(querySQL);
//        tableEnvironment.toRetractStream(tableEnvironment.sqlQuery(querySQL), Row.class).print();
        tableEnvironment.sqlUpdate(querySQL);
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
