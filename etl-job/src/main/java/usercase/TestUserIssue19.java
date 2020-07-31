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
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

import java.sql.Date;
import java.time.LocalDate;

public class TestUserIssue19 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        environment.setParallelism(1);
        environment.enableCheckpointing(200);

        tableEnvironment.executeSql("create table test_tbl ( " +
            "        `monitorId` STRING,\n" +
            "        `deviceId` STRING,\n" +
            "        `state` DOUBLE ) with ( " +
            "        'connector' = 'filesystem',\n" +
            "        'path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user19.json',\n" +
            "        'format' = 'json')");
        //tableEnvironment.executeSql("select  SPLIT_INDEX(deviceId, ';', 0) from test_tbl").print();
        tableEnvironment.executeSql("select  SPLIT_INDEX(deviceId, U&'\\003B', 0) from test_tbl").print();

    }

    public static class Int2DateFunc extends ScalarFunction {

        public Date eval(int epochDay) {
            return Date.valueOf(LocalDate.ofEpochDay(epochDay));
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return super.getTypeInference(typeFactory);
        }
    }
}
