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
import org.apache.flink.util.CloseableIterator;


public class TestUserIssue15 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        env.setParallelism(1);

        tableEnv.executeSql("CREATE TABLE test (\n" +
            "store_id INT,\n" +
            "store_type VARCHAR,\n" +
            "region_id INT,\n" +
            "store_name VARCHAR,\n" +
            "store_number INT,\n" +
            "store_street_address VARCHAR,\n" +
            "store_city VARCHAR,\n" +
            "store_state VARCHAR,\n" +
            "store_postal_code VARCHAR,\n" +
            "store_country VARCHAR,\n" +
            "store_manager VARCHAR,\n" +
            "store_phone VARCHAR,\n" +
            "store_fax VARCHAR,\n" +
            "first_opened_date TIMESTAMP,\n" +
            "last_remodel_date TIMESTAMP,\n" +
            "store_sqft INT,\n" +
            "grocery_sqft INT,\n" +
            "frozen_sqft INT,\n" +
            "meat_sqft INT,\n" +
            "coffee_bar BOOLEAN,\n" +
            "video_store BOOLEAN,\n" +
            "salad_bar BOOLEAN,\n" +
            "prepared_food BOOLEAN,\n" +
            "florist BOOLEAN" +
            ") WITH (" +
            " 'connector' = 'filesystem',\n" +
            " 'path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/test15.csv',\n" +
            " 'format' = 'csv'," +
            "  'csv.field-delimiter' = '|'," +
            "  'csv.null-literal'=''" +
            ")");
        CloseableIterator<Row> it = tableEnv.executeSql(" SELECT * FROM test").collect();

        while(it.hasNext()) {
            System.out.println(it.next());
        }
    }
}
