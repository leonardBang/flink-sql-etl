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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;

import java.sql.Timestamp;

public class testUserIssue7 {
    public static void main(String[] args) throws Exception {
        long t = System.currentTimeMillis();
        System.out.println(t);
        System.out.println(SqlDateTimeUtils.fromUnixtime(t/1000));
        System.out.println(SqlDateTimeUtils.toTimestamp(SqlDateTimeUtils.fromUnixtime(t/1000)));

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        EnvironmentSettings environmentSettings = new EnvironmentSettings.Builder()
            .inStreamingMode()
            .useBlinkPlanner()
            .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, environmentSettings);
        String sourceDDL = "CREATE TABLE user_behavior (\n" +
            "            itemCode VARCHAR,\n" +
            "\n" +
            "            ts BIGINT COMMENT '时间戳',\n" +
            "\n" +
            "            t as TO_TIMESTAMP(FROM_UNIXTIME(ts /1000,'yyyy-MM-dd HH:mm:ss')),\n" +
            "\n" +
            "            proctime as PROCTIME(),\n" +
            "\n" +
            "            WATERMARK FOR t as t - INTERVAL '5' SECOND\n" +
            "\n" +
            ") WITH (\n" +
            "\n" +
            "            'connector.type' = 'kafka',\n" +
            "\n" +
            "            'connector.version' = '0.10',\n" +
            "\n" +
            "            'connector.topic' = 'scan-flink-topic',\n" +
            "\n" +
            "            'connector.properties.group.id' ='qrcode_pv_five_min',\n" +
            "\n" +
            "            'connector.startup-mode' = 'latest-offset',\n" +
            "\n" +
            "            'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
            "\n" +
            "            'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
            "\n" +
            "            'update-mode' = 'append',\n" +
            "\n" +
            "            'format.type' = 'json',\n" +
            "\n" +
            "            'format.derive-schema' = 'true'\n" +
            "\n" +
            "        )";
        String sinkDDL = "CREATE TABLE pv_five_min (\n" +
            "            item_code VARCHAR,\n" +
            "            dt VARCHAR,\n" +
            "            dd VARCHAR,\n" +
            "            pv BIGINT\n" +
            "        ) WITH (\n" +
            "            'connector.type' = 'jdbc',\n" +
            "            'connector.url' = 'jdbc:mysql://127.0.0.1:3306/test',\n" +
            "            'connector.table' = 'qrcode_pv_five_min',\n" +
            "            'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
            "            'connector.username' = 'root',\n" +
            "            'connector.password' = 'root',\n" +
            "            'connector.write.flush.max-rows' = '1'\n" +
            "        )";

        String query = "INSERT INTO pv_five_min\n" +
            "            SELECT\n" +
            "        itemCode As item_code,\n" +
            "            DATE_FORMAT(TUMBLE_START(t, INTERVAL '1' MINUTE),'yyyy-MM-dd HH:mm') dt,\n" +
            "            DATE_FORMAT(TUMBLE_END(t, INTERVAL '1' MINUTE),'yyyy-MM-dd HH:mm') dd,\n" +
            "            COUNT(*) AS pv\n" +
            "        FROM user_behavior\n" +
            "        GROUP BY TUMBLE(t, INTERVAL '1' MINUTE),itemCode";

        tableEnvironment.sqlUpdate(sourceDDL);
        tableEnvironment.sqlUpdate(sinkDDL);
        tableEnvironment.sqlUpdate(query);
        tableEnvironment.execute("user_case");
    }
}
