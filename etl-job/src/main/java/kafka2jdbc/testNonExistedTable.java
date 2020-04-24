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

package kafka2jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class testNonExistedTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);

        String csvSourceDDL = "create table csv(" +
                " id INT," +
                " note VARCHAR," +
                " country VARCHAR," +
                " record_time TIMESTAMP(3)," +
                " doub_val DECIMAL(38, 18)," +
                " date_val DATE," +
                " time_val TIME(0)" +
                ") with (" +
                " 'connector.type' = 'filesystem',\n" +
                " 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/test_nonexistedTable.csv',\n" +
                " 'format.type' = 'csv'" +
                ")";
        String mysqlSinkDDL = "CREATE TABLE nonExisted (\n" +
                "  c0 BOOLEAN," +
                "  c1 INTEGER," +
                "  c2 BIGINT," +
                "  c3 FLOAT," +
                "  c4 DOUBLE," +
                "  c5 DECIMAL(38, 18)," +
                "  c6 VARCHAR," +
                "  c7 DATE," +
                "  c8 TIME(0)," +
                "  c9 TIMESTAMP(3)" +
                ") WITH (\n" +
                "   'connector.type' = 'jdbc',\n" +
                "   'connector.url' = 'jdbc:mysql://localhost:3306/test',\n" +
                "   'connector.username' = 'root'," +
                "   'connector.table' = 'nonExisted',\n" +
                "   'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
                "   'connector.write.auto-create-table' = 'true' " +
                ")";
        String query = "insert into nonExisted " +
                "select true as c0, id, cast(id as bigint),cast(doub_val as float),cast(doub_val as double), doub_val,country," +
                " date_val, time_val, record_time from csv";
        tableEnvironment.sqlUpdate(csvSourceDDL);
        tableEnvironment.sqlUpdate(mysqlSinkDDL);
        tableEnvironment.sqlUpdate(query);
        tableEnvironment.execute("csvTest");
    }
}
