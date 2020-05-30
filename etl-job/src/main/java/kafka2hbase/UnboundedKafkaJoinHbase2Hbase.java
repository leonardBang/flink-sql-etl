package kafka2hbase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
        testJoinDDLHbaseWithFunction(tableEnvironment);
    }


    private static void testJoinDDLHbaseWithFunction(StreamTableEnvironment tableEnvironment) throws Exception {
        tableEnvironment.sqlUpdate(FlinkSqlConstants.ordersTableDDL);
        tableEnvironment.sqlUpdate(FlinkSqlConstants.mysqlCurrencyDDL);
        tableEnvironment.sqlUpdate(FlinkSqlConstants.hbaseCountryDDLWithFunction);

        String sinkTableDDL = "CREATE TABLE gmv (\n" +
                "  rowkey VARCHAR,\n" +
                "  f1 ROW<log_ts VARCHAR,item VARCHAR,country_name VARCHAR> \n" +
                ") WITH (\n" +
                "    'connector.type' = 'hbase',\n" +
                "    'connector.version' = '1.4.3',\n" +
                "    'connector.table-name' = 'gmv',\n" +
                "    'connector.zookeeper.quorum' = 'localhost:2182',\n" +
                "    'connector.zookeeper.znode.parent' = '/hbase',\n" +
                "    'connector.write.buffer-flush.max-size' = '10mb', \n" +
                "    'connector.write.buffer-flush.max-rows' = '1000',  \n" +
                "    'connector.write.buffer-flush.interval' = '2s' " +
                ")";
        tableEnvironment.sqlUpdate(sinkTableDDL);

        String querySQL =
                " insert into gmv \n" +
                " select  rowkey, ROW(max(ts), max(item), max(country_name)) as f1\n" +
                " from (" +
                "select concat(cast(o.ts as VARCHAR), '_', item, '_', co.f1.country_name) as rowkey,\n" +
                        " cast(o.ts as VARCHAR) as ts, o.item as item, co.f1.country_name as country_name\n" +
                        " from orders as o \n" +
                        " left outer join currency FOR SYSTEM_TIME AS OF o.proc_time c\n" +
                        " on o.currency = c.currency_name\n" +
                        " left outer join country FOR SYSTEM_TIME AS OF o.proc_time co\n" +
                        " on c.country = co.rowkey \n" +
                ") a group by rowkey\n" ;

        tableEnvironment.sqlUpdate(querySQL);

        System.out.println(FlinkSqlConstants.ordersTableDDL);
        System.out.println(FlinkSqlConstants.mysqlCurrencyDDL);
        System.out.println(FlinkSqlConstants.hbaseCountryDDLWithFunction);
        System.out.println(sinkTableDDL);
        System.out.println(querySQL);

        tableEnvironment.execute("KafkaJoinHbase2Hbase");
    }
}
