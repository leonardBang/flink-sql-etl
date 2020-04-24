///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package kafka2jdbc;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
//public class retract2Mysql {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);
//
//        tenv.sqlUpdate("CREATE TABLE orders " +
//                "            (" +
//                "               order_no     string," +
//                "               order_state  int," +
//                "               pay_time     string," +
//                "               create_time  string," +
//                "               update_time  string" +
//                "             ) " +
//                "       WITH (" +
//                "               'connector.type' = 'kafka',       " +
//                "               'connector.version' = '0.10', " +//--kafka版本
//                "               'connector.topic' = 'orders_4'," +//--kafkatopic
//                "               'connector.properties.zookeeper.connect' = 'localhost:2181', " +
//                "               'connector.properties.bootstrap.servers' = 'localhost:9092'," +
//                "               'connector.properties.group.id' = 'testGroup'," +
//                "               'connector.startup-mode' = 'earliest-offset'," +
//                "               'format.type' = 'json'  " +//--数据为json格式
//                "             )");
//        tenv.sqlUpdate("CREATE TABLE order_detail " +
//                "            (" +
//                "               order_no     string," +
//                "               product_code string," +
//                "               quantity     int," +
//                "               create_time  string," +
//                "               update_time  string" +
//                "             ) " +
//                "       WITH (" +
//                "               'connector.type' = 'kafka', " +
//                "               'connector.version' = '0.10',  " +//--kafka版本
//                "               'connector.topic' = 'order_details_5'," +//--kafkatopic
//                "               'connector.properties.zookeeper.connect' = 'localhost:2181', " +
//                "               'connector.properties.bootstrap.servers' = 'localhost:9092'," +
//                "               'connector.properties.group.id' = 'testGroup'," +
//                "               'connector.startup-mode' = 'earliest-offset'," +
//                "               'format.type' = 'json'  " +//--数据为json格式
//                "             )");
////        tenv.toAppendStream(tenv.sqlQuery("select * from order_detail"), Row.class).print();
////        tenv.execute("test");
//
//        tenv.sqlUpdate("CREATE TABLE product_sale" +
//                "             (" +
//                "              order_date string," +
//                "              product_code string," +
//                "              cnt int" +
//                "              ) " +
//                "         WITH (" +
//                "           'connector.type' = 'jdbc', " +
//                "           'connector.url' = 'jdbc:mysql://localhost:3306/test', " +
//                "           'connector.table' = 'order_state_cnt', " +
//                "           'connector.driver' = 'com.mysql.jdbc.Driver', " +
//                "           'connector.username' = 'root'," +
//                "           'connector.write.flush.max-rows' = '1'," +//--默认每5000条数据写入一次，测试调小一点
//                "           'connector.write.flush.interval' = '2s'," +//--写入时间间隔
//                "           'connector.write.max-retries' = '3'" +
//                "         )");
//        tenv.sqlUpdate("insert into product_sale " +
//                "select create_date,product_code,sum(quantity)" +
//                "from (select t1.order_no," +
//                "             t1.create_date," +
//                "             t2.product_code," +
//                "             t2.quantity" +
//                "       from (select order_no," +
//                "                    order_state," +
//                "                    substring(create_time,1,10) create_date," +
//                "                    update_time ," +
//                "                    row_number() over(partition by order_no order by update_time desc) as rn" +
//                "              from orders" +
//                "              )t1" +
//                "       left join order_detail t2" +
//                "            on t1.order_no=t2.order_no" +
//                "      where t1.rn=1" +//--取最新的订单状态数据
//                "      and t1.order_state<>0" +//--不包含取消订单
//                "   )t3" +
//                " group by create_date,product_code");
////
////        Table table = tenv.sqlQuery("select create_date,product_code,sum(quantity)" +
////                "from (select t1.order_no," +
////                "             t1.create_date," +
////                "             t2.product_code," +
////                "             t2.quantity" +
////                "       from (select order_no," +
////                "                    order_state," +
////                "                    substring(create_time,1,10) create_date," +
////                "                    update_time ," +
////                "                    row_number() over(partition by order_no order by update_time desc) as rn" +
////                "              from orders" +
////                "              )t1"x +
////                "       left join order_detail t2" +
////                "            on t1.order_no=t2.order_no" +
////                "      where t1.rn=1" +
////                "      and t1.order_state<>0" +
////                "   )t3" +
////                " group by create_date,product_code");
////        tenv.toRetractStream(table, Row.class).print();
//        tenv.execute("count");
//    }
//}
