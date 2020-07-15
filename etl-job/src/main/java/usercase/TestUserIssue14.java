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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class TestUserIssue14 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        env.setParallelism(1);

        final Table inputTable = tableEnv.fromValues(//
            DataTypes.ROW(//
                DataTypes.FIELD("col1", DataTypes.STRING()), //
                DataTypes.FIELD("col2", DataTypes.STRING())//
            ), //
            Row.of(1L, "Hello"), //
            Row.of(2L, "Hello"), //
            Row.of(3L, ""), //
            Row.of(4L, "Ciao"));
        tableEnv.createTemporaryView("ParquetDataset", inputTable);
        tableEnv.executeSql(//
            "CREATE TABLE `out` (\n" + //
                "col1 STRING,\n" + //
                "col2 STRING\n" + //
                ") WITH (\n" + //
                " 'connector' = 'filesystem',\n" + //
                " 'format' = 'parquet',\n" + //
                " 'path' = 'file:///Users/bang/test',\n" + //
                " 'sink.shuffle-by-partition.enable' = 'true'\n" + //
                ")");

        tableEnv.executeSql("INSERT INTO `out` SELECT * FROM ParquetDataset").getJobClient()
        .get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
    }
}
