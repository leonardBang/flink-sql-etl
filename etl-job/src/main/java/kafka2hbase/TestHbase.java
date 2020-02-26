package kafka2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


public class TestHbase {
    public static void main(String[] args) throws Exception {
         beforTest();
//        test();
    }

    private static void beforTest() throws Exception {
        // create 'country','f1','f2'
        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2182");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
        HTable table = new HTable(config, "country");

        Put put1 = new Put("America".getBytes());
        put1.addColumn("f1".getBytes(), "country_id".getBytes(), Bytes.toBytes(1));
        put1.addColumn("f1".getBytes(), "country_name".getBytes(), "America".getBytes());
        put1.addColumn("f1".getBytes(), "country_name_cn".getBytes(), "美国".getBytes());
        put1.addColumn("f1".getBytes(), "currency".getBytes(), "US Dollar".getBytes()) ;
        put1.addColumn("f1".getBytes(), "region_name".getBytes(), "北美洲".getBytes());
         put1.addColumn("f2".getBytes(), "record_timestamp3".getBytes(), "2019-08-18 19:02:00.123".getBytes());
        put1.addColumn("f2".getBytes(), "record_timestamp9".getBytes(),  "2019-08-18 19:02:00.123456789".getBytes());
        put1.addColumn("f2".getBytes(), "time3".getBytes(),  "19:02:00getBytes.123".getBytes());
        put1.addColumn("f2".getBytes(), "time9".getBytes(),  "19:02:00getBytes.123456789".getBytes());
        put1.addColumn("f2".getBytes(), "gdp".getBytes(), Bytes.toBytes(1000023.1230));

        Put put2 = new Put("China".getBytes());
        put2.addColumn("f1".getBytes(), "country_id".getBytes(), Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(), "country_name".getBytes(), "China".getBytes());
        put2.addColumn("f1".getBytes(), "country_name_cn".getBytes(), "中国".getBytes());
        put2.addColumn("f1".getBytes(), "currency".getBytes(), "RMB".getBytes()) ;
        put2.addColumn("f1".getBytes(), "region_name".getBytes(), "亚洲".getBytes());
        put2.addColumn("f2".getBytes(), "record_timestamp3".getBytes(), "2019-08-18 19:02:00.123".getBytes());
        put2.addColumn("f2".getBytes(), "record_timestamp9".getBytes(),  "2019-08-18 19:02:00.123456789".getBytes());
        put2.addColumn("f2".getBytes(), "time3".getBytes(),  "19:02:00getBytes.123".getBytes());
        put2.addColumn("f2".getBytes(), "time9".getBytes(),  "19:02:00getBytes.123456789".getBytes());
        put2.addColumn("f2".getBytes(), "gdp".getBytes(), Bytes.toBytes(900023.1230));

        Put put3 = new Put("Japan".getBytes());
        put3.addColumn("f1".getBytes(), "country_id".getBytes(), Bytes.toBytes(3));
        put3.addColumn("f1".getBytes(), "country_name".getBytes(), "Japan".getBytes());
        put3.addColumn("f1".getBytes(), "country_name_cn".getBytes(), "日本".getBytes());
        put3.addColumn("f1".getBytes(), "currency".getBytes(), "YEN".getBytes()) ;
        put3.addColumn("f1".getBytes(), "region_name".getBytes(), "亚洲".getBytes());
        put3.addColumn("f2".getBytes(), "record_timestamp3".getBytes(), "2019-08-18 19:02:00.123".getBytes());
        put3.addColumn("f2".getBytes(), "record_timestamp9".getBytes(),  "2019-08-18 19:02:00.123456789".getBytes());
        put3.addColumn("f2".getBytes(), "time3".getBytes(),  "19:02:00getBytes.123".getBytes());
        put3.addColumn("f2".getBytes(), "time9".getBytes(),  "19:02:00getBytes.123456789".getBytes());
        put3.addColumn("f2".getBytes(), "gdp".getBytes(), Bytes.toBytes(812345.1230));

        Put put4 = new Put("Germany".getBytes());
        put4.addColumn("f1".getBytes(), "country_id".getBytes(), Bytes.toBytes(4));
        put4.addColumn("f1".getBytes(), "country_name".getBytes(), "Germany".getBytes());
        put4.addColumn("f1".getBytes(), "country_name_cn".getBytes(), "德国".getBytes());
        put4.addColumn("f1".getBytes(), "currency".getBytes(), "Euro".getBytes()) ;
        put4.addColumn("f1".getBytes(), "region_name".getBytes(), "欧洲".getBytes());
        put4.addColumn("f2".getBytes(), "record_timestamp3".getBytes(), "2019-08-18 19:02:00.123".getBytes());
        put4.addColumn("f2".getBytes(), "record_timestamp9".getBytes(),  "2019-08-18 19:02:00.123456789".getBytes());
        put4.addColumn("f2".getBytes(), "time3".getBytes(),  "19:02:00getBytes.123".getBytes());
        put4.addColumn("f2".getBytes(), "time9".getBytes(),  "19:02:00getBytes.123456789".getBytes());
        put4.addColumn("f2".getBytes(), "gdp".getBytes(), Bytes.toBytes(9123456.1230));

        table.put(put1);
        table.put(put2);
        table.put(put3);
        table.put(put4);
        table.close();
        //get 'country','America'
    }

    public static void test() throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2182");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
        HTable table = new HTable(config, "country");
        // instantiate Get class
        Get g = new Get(Bytes.toBytes("Germany"));

        // get the Result object
        Result result = table.get(g);
        // read values from Result class object
        byte [] name = result.getValue(Bytes.toBytes("f1"),Bytes.toBytes("country_name"));
        byte [] name_cn = result.getValue(Bytes.toBytes("f1"),Bytes.toBytes("country_name_cn"));

        System.out.println("name: " + Bytes.toString(name));
        System.out.println("name_cn: " + Bytes.toString(name_cn));
    }
}
