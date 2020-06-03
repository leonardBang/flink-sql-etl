package usercase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TestUserIssue {
    private static String  kafkaOrdersDDL =  " CREATE TABLE yanfa_log (\n" +
        "   dt TIMESTAMP(3),\n" +
        "   conn_id STRING,\n" +
        "   sequence STRING,\n" +
        "   trace_id STRING,\n" +
        "   span_info STRING,\n" +
        "   service_id STRING,\n" +
        "   msg_id STRING,\n" +
        "   servicename STRING,\n" +
        "   ret_code STRING,\n" +
        "   duration STRING,\n" +
        "   req_body MAP<String,String>,\n" +
        "   res_body MAP<STRING,STRING>,\n" +
        "   extra_info MAP<STRING,STRING>,\n" +
        "   pt AS PROCTIME(),\n" +
        "   WATERMARK FOR dt AS dt - INTERVAL '60' SECOND\n" +
        ") WITH (\n" +
        "   'connector.type' = 'kafka',\n" +
        "   'connector.version' = '0.10',\n" +
        "   'connector.topic' = 'x-log-yanfa_log',\n" +
        "   'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
        "   'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
        "   'connector.startup-mode' = 'latest-offset',\n" +
        "   'update-mode' = 'append',\n" +
        "   'format.type' = 'json',\n" +
        "   'format.fail-on-missing-field' = 'true'\n" +
        ")";
    private static String hbaseDimTableDDL =
        "CREATE TABLE country (\n" +
         "  uid STRING,\n" +
         "  vehicle_level STRING,\n" +
         "  vin STRING,\n" +
         "  currency_time TIMESTAMP(3),\n" +
         "  country STRING,\n" +
         "  timestamp9 TIMESTAMP(3),\n" +
         "  time9 TIME(3),\n" +
         "  gdp DOUBLE\n" +
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
    private static String mysqlDimTableDDL =   "CREATE TABLE currency (\n" +
        "  currency_id BIGINT,\n" +
        "  vin STRING,\n" +
        "  rate DOUBLE,\n" +
        "  vehicle_level STRING,\n" +
        "  country STRING,\n" +
        "  timestamp9 TIMESTAMP(3),\n" +
        "  time9 TIME(3),\n" +
        "  gdp DOUBLE\n" +
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
    private static String query = "  select" +
        " TUMBLE_END(l.dt, INTERVAL '30' SECOND) as index_time," +
        " l.extra_info['cityCode'] as city_code," +
        " v.vehicle_level as vehicle_level," +
        " CAST(COUNT(DISTINCT req_body['driverId']) as STRING) as index_value " +
        "   from yanfa_log AS l LEFT JOIN country FOR SYSTEM_TIME AS OF l.pt AS d" +
        "    ON l.req_body['driverId'] = d.uid" +
        "  LEFT JOIN currency FOR SYSTEM_TIME AS OF l.pt AS v" +
        "  ON d.vin=v.vin " +
        "  where l.ret_code = '0' and l.servicename = 'MatchGtw.uploadLocationV4' and l.req_body['appId'] = 'saic_card'" +
        "  GROUP BY TUMBLE(l.dt, INTERVAL '30' SECOND), l.extra_info['cityCode'], v.vehicle_level";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);

        tableEnvironment.sqlUpdate(kafkaOrdersDDL);
        tableEnvironment.sqlUpdate(hbaseDimTableDDL);
        tableEnvironment.sqlUpdate(mysqlDimTableDDL);
        tableEnvironment.sqlQuery(query);

        //check the plan
        System.out.println(tableEnvironment.explain(tableEnvironment.sqlQuery(query)));

        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery(query), Row.class).print();
        tableEnvironment.execute("reproduce_user_issue");
    }
}
