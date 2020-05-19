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
        env.setParallelism(4);
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
                " doub_val DECIMAL(6, 2)," +
                " date_val DATE," +
                " time_val TIME" +
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
                "  c8 TIME," +
                "  c9 TIMESTAMP(3)" +
                ") WITH (\n" +
                "   'connector.type' = 'jdbc',\n" +
                "   'connector.url' = 'jdbc:mysql://localhost:3306/test',\n" +
                "   'connector.username' = 'root'," +
                "   'connector.table' = 'nonExisted3',\n" +
                "   'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
                "   'connector.write.auto-create-table' = 'true' " +
                ")";
        String query = "insert into nonExisted " +
                "select max(c0),c1,c2,c3,c4,max(c5),max(c6),max(c7),max(c8),max(c9) from " +
                " (select true as c0, id as c1, cast(id as bigint) as c2,cast(doub_val as float)as c3,cast(doub_val as double) as c4," +
                " doub_val as c5, country as c6, date_val as c7, time_val as c8, record_time as c9 from csv)" +
                " a group by c1, c2, c3, c4";
//        String query = "insert into nonExisted select true as c0, id as c1, cast(id as bigint) as c2,cast(doub_val as float)as c3,cast(doub_val as double) as c4," +
//                " doub_val as c5, country as c6, date_val as c7, time_val as c8, record_time as c9 from csv";
        tableEnvironment.sqlUpdate(csvSourceDDL);
        tableEnvironment.sqlUpdate(mysqlSinkDDL);
        tableEnvironment.sqlUpdate(query);
        tableEnvironment.execute("csvTest");
    }
}
