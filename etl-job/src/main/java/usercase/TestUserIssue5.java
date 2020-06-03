package usercase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;
import java.net.URLClassLoader;

public class TestUserIssue5 {

    private static String hbaseSourceDDL = "CREATE TABLE country (\n" +
            "  rowkey VARCHAR,\n" +
            "  f1 ROW<country_id INT, country_name VARCHAR, country_name_cn VARCHAR, currency VARCHAR, region_name VARCHAR> \n" +
            " " +
            ") WITH (\n" +
            "    'connector.type' = 'hbase',\n" +
            "    'connector.version' = '1.4.3',\n" +
            "    'connector.table-name' = 'country',\n" +
            "    'connector.zookeeper.quorum' = 'localhost:2182',\n" +
            "    'connector.zookeeper.znode.parent' = '/hbase' " +
            ")";
    public static void main(String[] args) throws Exception {

        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
            System.out.println(url.getFile());
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);
        tableEnvironment.sqlUpdate(hbaseSourceDDL);

        String querySQL = "select * from country\n";

        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery(querySQL), Row.class).print();

        tableEnvironment.execute("read_hbase_sql");
    }
}
