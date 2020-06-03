//package kafka2file;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.catalog.hive.HiveCatalog;
//import org.apache.flink.types.Row;
//
//public class ReadHiveDataETL {
//    public static void main(String[] args) throws Exception{
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        executionEnvironment.setParallelism(1);
//        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment, environmentSettings);
//        testReadHive(tableEnvironment);
//    }
//
//    private static void testReadHive(StreamTableEnvironment tableEnvironment) throws Exception {
//        HiveCatalog hiveCatalog = new HiveCatalog("myhive", "default", "/Users/bang/hive-3.1.2/conf", "3.1.2");
//        tableEnvironment.registerCatalog("myhive", hiveCatalog);
//        tableEnvironment.useCatalog("myhive");
//        tableEnvironment.useDatabase("default");
//        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery("select * from user_info"), Row.class).print();
//        tableEnvironment.execute("readHive");
//    }
//
//}
