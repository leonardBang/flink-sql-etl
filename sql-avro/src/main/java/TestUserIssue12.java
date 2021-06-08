/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class TestUserIssue12 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        environment.setParallelism(1);

        //construct some test data with avro format
        //writeTestAvroData(tableEnvironment);

        tableEnvironment.executeSql("CREATE TABLE people (\n" +
            " name String," +
            " status Boolean," +
            " note STRING" +
            ") WITH (\n" +
            "    'connector' = 'filesystem',\n" +
            "    'path'     = 'file:///Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user.avro',\n" +
            "    'format'    = 'avro'\n" +
            ")");
        System.out.println("CREATE TABLE people (\n" +
            " name String," +
            " status Boolean," +
            " note STRING" +
            ") WITH (\n" +
            "    'connector' = 'filesystem',\n" +
            "    'path'     = 'file:///Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user.avro',\n" +
            "    'format'    = 'avro'\n" +
            ")");

        CloseableIterator<Row> result = tableEnvironment.executeSql("select * from people").collect();
        while (result.hasNext()) {
            System.out.println(result.next());
        }
    }

    private static void writeTestAvroData(StreamTableEnvironment tableEnvironment) throws Exception {
        String csvSourceDDL = "create table csv(" +
            " name String," +
            " status Boolean," +
            " note STRING" +
            ") with (" +
            " 'connector' = 'filesystem',\n" +
            " 'path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user.csv',\n" +
            " 'format' = 'csv'" +
            ")";
        String csvSink = "create table csvSink(" +
            " name String," +
            " status Boolean," +
            " note STRING" +
            ") with (" +
            " 'connector' = 'filesystem',\n" +
            " 'path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user.avro',\n" +
            " 'format' = 'avro'" +
            ")";
        tableEnvironment.executeSql(csvSourceDDL);
        tableEnvironment.executeSql(csvSink);
        tableEnvironment.executeSql("insert into csvSink select * from csv").await();
    }
}
