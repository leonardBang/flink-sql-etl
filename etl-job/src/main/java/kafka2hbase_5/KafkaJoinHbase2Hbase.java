package kafka2hbase_5;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import constants.FlinkSqlConstants;

public class KafkaJoinHbase2Hbase {
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
//        testJoinDDLHbase(tableEnvironment);
        testJoinHbaseWithPrecision(tableEnvironment);
//        testJoinDDLHbaseWithFunction(tableEnvironment);

    }

    private static void testJoinDDLHbase(TableEnvironment tableEnvironment) throws Exception {
        tableEnvironment.sqlUpdate(FlinkSqlConstants.hbaseCountryDDL);
        String sinkTableDDL = "CREATE TABLE gmv (\n" +
                "  rowkey VARCHAR,\n" +
                "  f1 ROW<log_ts VARCHAR,item VARCHAR,country_name VARCHAR,country_name_cn VARCHAR,region_name VARCHAR,\n" +
                "   currency VARCHAR,order_cnt BIGINT,currency_time TIMESTAMP(3), gmv DECIMAL(38,4)> \n" +
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

        String querySQL = " insert into gmv select concat(log_ts,'_',item) as rowkey,\n" +
                " ROW(log_ts, item, country_name, country_name_cn, region_name, currency, order_cnt, currency_time, gmv) as f1 from " +
                " (select  co.f1.country_name as country_name, co.f1.country_name_cn as country_name_cn," +
                " co.f1.region_name as region_name, co.f1.currency as currency," +
                " cast(TUMBLE_END(o.ts, INTERVAL '10' SECOND) as VARCHAR) as log_ts,\n" +
                " o.item, COUNT(o.order_id) as order_cnt, c.currency_time, cast(sum(o.amount_kg) * c.rate as DECIMAL(38, 4))  as gmv\n" +
                " from orders as o \n" +
                " left outer join currency FOR SYSTEM_TIME AS OF o.proc_time c\n" +
                " on o.currency = c.currency_name\n" +
                " left outer join country FOR SYSTEM_TIME AS OF o.proc_time co\n" +
                " on c.country = co.rowkey" +
                " group by o.item, c.currency_time, c.rate, co.f1.country_name, co.f1.country_name_cn, co.f1.region_name, co.f1.currency," +
                " TUMBLE(o.ts, INTERVAL '10' SECOND)) a\n" ;


        tableEnvironment.sqlUpdate(querySQL);

        tableEnvironment.execute("KafkaJoinHbase2Hbase");
    }

    private static void testJoinDDLHbaseWithFunction(TableEnvironment tableEnvironment) throws Exception {
        tableEnvironment.registerFunction("ExpandIdFunc", new ExpandIdFunc());

        tableEnvironment.sqlUpdate(FlinkSqlConstants.hbaseCountryDDLWithFunction);

        String sinkTableDDL = "CREATE TABLE gmv (\n" +
                "  rowkey VARCHAR,\n" +
                "  f1 ROW<log_ts VARCHAR,item VARCHAR,country_name VARCHAR,country_name_cn VARCHAR,region_name VARCHAR,\n" +
                "   currency VARCHAR,order_cnt BIGINT,currency_time TIMESTAMP(3), gmv DECIMAL(38, 18)> \n" +
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

        String querySQL = " insert into gmv select concat(log_ts,'_',item) as rowkey,\n" +
                " ROW(log_ts, item, country_name, country_name_cn, region_name, currency, order_cnt, currency_time, gmv) as f1 from " +
                " (select co.f1.country_name as country_name, co.f1.country_name_cn as country_name_cn," +
                " co.f1.region_name as region_name, co.f1.currency as currency," +
                " cast(TUMBLE_END(o.ts, INTERVAL '10' SECOND) as VARCHAR) as log_ts,\n" +
                " o.item, COUNT(o.order_id) as order_cnt, c.currency_time, cast(sum(o.amount_kg) * c.rate as DECIMAL(38, 18))  as gmv\n" +
                " from orders as o \n" +
                " left outer join currency FOR SYSTEM_TIME AS OF o.proc_time c\n" +
                " on o.currency = c.currency_name\n" +
                " left outer join country FOR SYSTEM_TIME AS OF o.proc_time co\n" +
                " on c.country = co.rowkey" +
                " group by o.item, c.currency_time, c.rate, co.f1.country_name, co.f1.country_name_cn, co.f1.region_name, co.f1.currency," +
                " TUMBLE(o.ts, INTERVAL '10' SECOND)) a\n" ;

        tableEnvironment.sqlUpdate(querySQL);

        tableEnvironment.execute("KafkaJoinHbase2Hbase");
    }

    private static void testSink(StreamTableEnvironment tableEnvironment) {
        String querySQL = "select  * from orders as o" +
                " left outer join country FOR SYSTEM_TIME AS OF o.proc_time c" +
                " on 1 = c.rowkey";
        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery(querySQL), Row.class).print();

    }

    private static void testJoinHbaseWithPrecision(StreamTableEnvironment tableEnvironment) throws Exception {
        tableEnvironment.sqlUpdate(FlinkSqlConstants.hbaseCountryDDLWithPrecison);

        String sinkTableDDL = "CREATE TABLE gmv (\n" +
                "  rowkey VARCHAR,\n" +
                "  f1 ROW<record_timestamp9 TIMESTAMP(9), gdp DECIMAL(38, 4)> \n" +
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

        String querySQL = "select * from gmv" ;

        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery(querySQL), Row.class).print();

        tableEnvironment.execute("KafkaJoinHbase2Hbase");
    }

    public static class ExpandIdFunc extends ScalarFunction {
        public String eval(Integer t) {
            return "test_" + t;
        }

        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
