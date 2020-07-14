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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestUserIssue12 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        environment.setParallelism(1);

        tableEnvironment.executeSql("create table csv( pageId VARCHAR, eventId VARCHAR, recvTime VARCHAR) with ( 'connector' = 'filesystem',\n" +
            " 'path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user3.csv',\n" +
            " 'format' = 'csv')");
        tableEnvironment.executeSql("CREATE TABLE es_table (\n" +
            "  aggId varchar ,\n" +
            "  pageId varchar ,\n" +
            "  ts varchar ,\n" +
            "  expoCnt int ,\n" +
            "  clkCnt int\n" +
            ") WITH (\n" +
            "'connector' = 'elasticsearch-6',\n" +
            "'hosts' = 'http://localhost:9200',\n" +
            "'index' = 'usercase111',\n" +
            "'document-type' = '_doc',\n" +
            "'document-id.key-delimiter' = '$',\n" +
            "'sink.bulk-flush.interval' = '1000',\n" +
            "'format' = 'json'\n" +
            ")");
        Table res = tableEnvironment.sqlQuery(" SELECT  pageId,eventId,cast(recvTime as varchar) as ts, 1, 1 from csv");
        TableResult tableResult = res.executeInsert("es_table");
        tableResult.getJobClient().get();


    }
}
