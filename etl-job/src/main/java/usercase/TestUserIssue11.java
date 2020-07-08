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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class TestUserIssue11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        tableEnvironment.sqlUpdate("CREATE TABLE mysource (\n" +
            "    parsedResponse ARRAY<STRING>,\n" +
            "    `timestamp` BIGINT\n" +
            ") WITH (" +
            " 'connector.type' = 'kafka',\n" +
            " 'connector.version' = '0.10',\n" +
            "  'update-mode' = 'append',   " +
            "  'connector.topic' = 'mysource1',\n" +
            "  'connector.properties.0.key' = 'zookeeper.connect',\n" +
            "  'connector.properties.0.value' = 'localhost:2181',\n" +
            "  'connector.properties.1.key' = 'bootstrap.servers',\n" +
            "  'connector.properties.1.value' = 'localhost:9092',\n" +
            "  'connector.properties.2.key' = 'group.id',\n" +
            "  'connector.properties.2.value' = 'testGroup',\n" +
            "  'connector.startup-mode' = 'earliest-offset', " +
            " 'format.type' = 'json'," +
            " 'format.derive-schema' = 'true'" +
            ")");
        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery("select `timestamp`,fruit from  mysource, UNNEST(mysource.parsedResponse) AS A(fruit) "), Row.class)
            .print();
        tableEnvironment.execute("");
        // output:
        //7> 1522253345,apple
        //7> 1522253345,banana
        //7> 1522253345,orange
        //7> 1522253345,apple
        //7> 1522253345,banana
        //7> 1522253345,orange
        //7> 1522253345,apple
        //7> 1522253345,banana
        //7> 1522253345,orange
    }
}
