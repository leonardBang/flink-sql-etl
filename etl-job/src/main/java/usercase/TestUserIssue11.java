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

package usercase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.time.Instant;

public class TestUserIssue11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.executeSql("CREATE TABLE people (\n" +
            "    user_name  STRING,\n" +
            "    content STRING\n" +
            ") WITH (\n" +
            "    'connector' = 'filesystem',\n" +
            "    'path'     = 'file:///Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/avro/UserAvro.avsc',\n" +
            "    'format'    = 'avro',\n" +
            "    'record-class'    = 'avro.Person',\n" +
            "    'property-version'    = '1',\n" +
            "    'properties.bootstrap.servers' = 'kafka:9092'\n" +
            ")");

        System.out.println("CREATE TABLE people (\n" +
            "    user_name  STRING,\n" +
            "    content STRING\n" +
            ") WITH (\n" +
            "    'connector' = 'filesystem',\n" +
            "    'path'     = 'file:///Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/avro/UserAvro.avsc',\n" +
            "    'format'    = 'avro',\n" +
            "    'record-class'    = 'avro.Person',\n" +
            "    'property-version'    = '1',\n" +
            "    'properties.bootstrap.servers' = 'kafka:9092'\n" +
            ")");


        tableEnvironment.executeSql("select * from people");
    }
}
