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
//package usercase;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.StatementSet;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.catalog.DataTypeFactory;
//import org.apache.flink.table.functions.ScalarFunction;
//import org.apache.flink.table.types.inference.TypeInference;
//
//import java.sql.Date;
//import java.time.LocalDate;
//
//public class TestUserIssue18 {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
//        environment.setParallelism(1);
//
//
//        tableEnvironment.executeSql("create table online_example (\n" +
//            "    face_id varchar,\n" +
//            "    device_id varchar,\n" +
//            "    feature_data double\n" +
//            ") with (\n" +
//            "    'connector' = 'kafka',\n" +
//            "    'topic' = 'json-test-2',\n" +
//            "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
//            "    'properties.group.id' = 'read_example',\n" +
//            "    'format' = 'csv',\n" +
//            "    'csv.field-delimiter' = ' '," +
//            "    'scan.startup.mode' = 'earliest-offset'                 \n" +
//            ")");
//        tableEnvironment.executeSql("create table write_example (\n" +
//            "     face_id varchar,\n" +
//            "     device_id varchar " +
//            " ) with (\n" +
//            "     'connector' = 'kafka',\n" +
//            "     'topic' = 'tianchi_write_example-3',\n" +
//            "     'properties.bootstrap.servers' = 'localhost:9092',\n" +
//            "     'properties.group.id' = 'write_example',\n" +
//            "     'format' = 'csv',\n" +
//            "     'scan.startup.mode' = 'earliest-offset'\n" +
//            " )");
//
//        StatementSet statementSet = tableEnvironment.createStatementSet();
//        statementSet.addInsertSql("insert into write_example SELECT  face_id, device_id from online_example");
//
//        statementSet.execute().getJobClient().get()
//       .getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
//    }
//
//    public static class Int2DateFunc extends ScalarFunction {
//
//        public Date eval(int epochDay) {
//            return Date.valueOf(LocalDate.ofEpochDay(epochDay));
//        }
//
//        @Override
//        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
//            return super.getTypeInference(typeFactory);
//        }
//    }
//}
