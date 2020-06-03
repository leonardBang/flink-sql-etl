package kafka2hbase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import constants.FlinkSqlConstants;

public class UnboundedKafkaJoinHbase2Hbase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);
        testJoinDDLHbaseWithFunction(env, tableEnvironment);
     }


    private static void testJoinDDLHbaseWithFunction(StreamExecutionEnvironment env, StreamTableEnvironment tableEnvironment) throws Exception {
        tableEnvironment.sqlUpdate(FlinkSqlConstants.ordersTableDDL11);
        tableEnvironment.sqlUpdate(FlinkSqlConstants.mysqlCurrencyDDL11);
        tableEnvironment.sqlUpdate(FlinkSqlConstants.hbaseCountryDDLWithPrecison11);

        String sinkTableDDL = "CREATE TABLE gmv (\n" +
                "  rowkey VARCHAR,\n" +
                "  f1 ROW<log_ts VARCHAR,item VARCHAR,country_name VARCHAR>," +
                "  f2 ROW<record_timestamp3 TIMESTAMP(3), record_timestamp9 TIMESTAMP(3), time3 TIME(3), time9 TIME(9), gdp DECIMAL(10,4)>" +
                ") WITH (\n" +
                "    'connector' = 'hbase-1.4',\n" +
                "    'table-name' = 'gmv',\n" +
                "    'zookeeper.quorum' = 'localhost:2182',\n" +
                "    'zookeeper.znode-parent' = '/hbase',\n" +
                "    'sink.buffer-flush.max-size' = '10mb', \n" +
                "    'sink.buffer-flush.max-rows' = '1000',  \n" +
                "    'sink.buffer-flush.interval' = '2s' " +
                ")";
        tableEnvironment.sqlUpdate(sinkTableDDL);

        //test lookup
        String querySQL =
                " select  rowkey, ROW(max(ts), max(item), max(country_name)) as f1, max(gdp), max(record_timestamp3)\n" +
                " from (" +
                "select concat(cast(o.ts as VARCHAR), '_', item, '_', co.f1.country_name) as rowkey,\n" +
                        " cast(o.ts as VARCHAR) as ts, o.item as item, co.f1.country_name as country_name," +
                        "co.gdp as gdp, co.record_timestamp3 as record_timestamp3\n" +
                        " from orders as o \n" +
                        " left outer join currency FOR SYSTEM_TIME AS OF o.proc_time c\n" +
                        " on o.currency = c.currency_name\n" +
                        " left outer join country FOR SYSTEM_TIME AS OF o.proc_time co\n" +
                        " on c.country = co.rowkey \n" +
                ") a group by rowkey\n" ;

        tableEnvironment.toRetractStream(tableEnvironment.sqlQuery(querySQL), Row.class).print();
        env.execute();


//        test source
//        tableEnvironment.toRetractStream(tableEnvironment.sqlQuery("select * from (select rowkey, f1.country_name,f1.country_name_cn, f2.record_timestamp3,f2.record_timestamp9, f2.gdp from country) a "), Row.class)
//            .print();
//        env.execute();

//        tableEnvironment.execute("KafkaJoinHbase2Hbase");
    }
}
