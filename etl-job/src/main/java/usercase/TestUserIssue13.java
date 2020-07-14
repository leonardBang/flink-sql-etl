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

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;

public class TestUserIssue13 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        environment.setStateBackend(new FsStateBackend(""));
        tableEnvironment.getConfig().getConfiguration().set(CHECKPOINTS_DIRECTORY, "your-cp-path");
        environment.setParallelism(1);

        tableEnvironment.executeSql("create table jsonT ( " +
            "        `monitorId` STRING,\n" +
            "        `deviceId` STRING,\n" +
            "        `state` INT,\n" +
            "        `time_st` TIMESTAMP(3),\n" +
            "        WATERMARK FOR time_st AS time_st - INTERVAL '2' SECOND,\n" +
            "        `data` DOUBLE) with ( 'connector' = 'filesystem',\n" +
            "       'path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user4.json',\n" +
            "       'format' = 'json')");
        CloseableIterator<Row> tableResult = tableEnvironment.executeSql(" SELECT  * from jsonT").collect();
        while(tableResult.hasNext()) {
            System.out.println(tableResult.next());
        }
    }
}
