//package kafka2file;
//
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.environment.CheckpointConfig;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.catalog.CatalogBaseTable;
//import org.apache.flink.table.catalog.CatalogTable;
//import org.apache.flink.table.catalog.CatalogTableImpl;
//import org.apache.flink.table.catalog.ObjectPath;
//import org.apache.flink.table.catalog.hive.HiveCatalog;
//import org.apache.flink.types.Row;
//
//import java.util.Map;
//
//import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
//import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;
//
//public class StreamETLKafka2HdfsSQL {
//    public static void main(String[] args) throws Exception {
//        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        executionEnvironment.enableCheckpointing(1000);
//        executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//        executionEnvironment.getCheckpointConfig().setCheckpointTimeout(60000);
//        executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        executionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        executionEnvironment.getCheckpointConfig().setPreferCheckpointForRecovery(true);
//
//
//        executionEnvironment.setParallelism(1);
//        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment, environmentSettings);
//        testHiveSink(tableEnvironment);
////        testHivePartition(tableEnvironment);
//
//        testKafka2hive(tableEnvironment);
//    }
//
//    private static void testHiveSink(StreamTableEnvironment tableEnvironment) throws Exception{
//        String csvSourceDDL = "create table csv( " +
//                "user_name VARCHAR, " +
//                "is_new BOOLEAN, " +
//                "content VARCHAR) with ( " +
//                " 'connector.type' = 'filesystem',\n" +
//                " 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user.csv',\n" +
//                " 'format.type' = 'csv',\n" +
//                " 'format.fields.0.name' = 'user_name',\n" +
//                " 'format.fields.0.data-type' = 'STRING',\n" +
//                " 'format.fields.1.name' = 'is_new',\n" +
//                " 'format.fields.1.data-type' = 'BOOLEAN',\n" +
//                " 'format.fields.2.name' = 'content',\n" +
//                " 'format.fields.2.data-type' = 'STRING')";
//        System.out.println(csvSourceDDL);
//
//        HiveCatalog hiveCatalog = new HiveCatalog("myhive", "default", "/Users/bang/hive-3.1.2/conf", "3.1.2");
//        tableEnvironment.registerCatalog("myhive", hiveCatalog);
//        tableEnvironment.useCatalog("myhive");
//        tableEnvironment.useDatabase("default");
//        tableEnvironment.sqlUpdate(csvSourceDDL);
//        String hiveTableDDL = "insert into user_ino_no_part select user_name, is_new, content from csv";
//
//        tableEnvironment.sqlUpdate(hiveTableDDL);
//        tableEnvironment.execute("StreamETLKafka2HdfsSQL");
//    }
//
//    private static void testHivePartition(StreamTableEnvironment tableEnvironment) throws Exception{
//        String csvSourceDDL = "create table csv( " +
//                "user_name VARCHAR, " +
//                "is_new BOOLEAN, " +
//                "content VARCHAR, " +
//                "date_col VARCHAR) with ( " +
//                " 'connector.type' = 'filesystem',\n" +
//                " 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user_part.csv',\n" +
//                " 'format.type' = 'csv',\n" +
//                " 'format.fields.0.name' = 'user_name',\n" +
//                " 'format.fields.0.data-type' = 'STRING',\n" +
//                " 'format.fields.1.name' = 'is_new',\n" +
//                " 'format.fields.1.data-type' = 'BOOLEAN',\n" +
//                " 'format.fields.2.name' = 'content',\n" +
//                " 'format.fields.2.data-type' = 'STRING',\n" +
//                " 'format.fields.3.name' = 'date_col',\n" +
//                " 'format.fields.3.data-type' = 'STRING')";
//        System.out.println(csvSourceDDL);
//
//
//        HiveCatalog hiveCatalog = new HiveCatalog("myhive", "hive_test", "/Users/bang/hive-3.1.2/conf", "3.1.2");
//        tableEnvironment.registerCatalog("myhive", hiveCatalog);
//        tableEnvironment.sqlUpdate(csvSourceDDL);
////        dynamic partition
////        String hiveTableDDL = "insert into myhive.hive_test.user_info_partition select user_name, is_new, content, date_col from csv";
//        String hiveTableDDL = "insert into myhive.hive_test.user_info_partition PARTITION(date_col='2020-01-01') select user_name, is_new, content from csv";
//        tableEnvironment.sqlUpdate(hiveTableDDL);
//
//        tableEnvironment.execute("StreamETLKafka2HdfsSQL");
//    }
//
//    private static void testKafka2hive(StreamTableEnvironment tableEnvironment) throws Exception{
//        String soureTableDDL = "CREATE TABLE csvData (\n" +
//                "  user_name STRING,\n" +
//                "  is_new    BOOLEAN,\n" +
//                "  content STRING,\n" +
//                "  date_col STRING" +
//                ") WITH (\n" +
//                "  'connector.type' = 'kafka',\n" +
//                "  'connector.version' = '0.10',\n" +
//                "  'connector.topic' = 'csv_data',\n" +
//                "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
//                "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
//                "  'connector.properties.group.id' = 'testGroup-1',\n" +
//                "  'connector.startup-mode' = 'earliest-offset',\n" +
//                "  'format.type' = 'csv')";
//
//        System.out.println(soureTableDDL);
//        HiveCatalog hiveCatalog = new HiveCatalog("myhive", "hive_test", "/Users/bang/hive-3.1.2/conf", "3.1.2");
//        tableEnvironment.registerCatalog("myhive", hiveCatalog);
//        tableEnvironment.sqlUpdate(soureTableDDL);
//
//        //set hive streaming table
//        ObjectPath path = new ObjectPath("hive_test", "user_info_kafka");
//        CatalogTableImpl catalogTable = (CatalogTableImpl)hiveCatalog.getTable(path);
//        Map<String, String> properties = catalogTable.getProperties();
//        properties.put(UPDATE_MODE, UPDATE_MODE_VALUE_APPEND);
//        CatalogTableImpl newTable = new CatalogTableImpl(catalogTable.getSchema(), properties, "newTable");
//        hiveCatalog.alterTable(path, newTable, false);
//        hiveCatalog.listTables("hive_test");
//
//        String hiveTableDDL = "insert into myhive.hive_test.user_info_kafka select user_name, is_new, content from csvData";
//        tableEnvironment.sqlUpdate(hiveTableDDL);
//        System.out.println(hiveTableDDL);
//
////      tableEnvironment.toAppendStream(tableEnvironment.sqlQuery("select user_name, is_new, content from csvData"), Row.class).print();
//        tableEnvironment.execute("StreamETLKafka2HdfsSQL");
//    }
//
//}
