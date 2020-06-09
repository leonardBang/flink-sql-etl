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

package pge2e;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.Arrays;

public class PgCatalogTest {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name            = "mypg";
        String defaultDatabase = "mydb";
        String username        = "postgres";
        String password        = "postgres";
        String baseUrl         = "jdbc:postgresql://localhost:5432/";

        JdbcCatalog catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl);
        tableEnv.registerCatalog("mypg", catalog);

        // set the JdbcCatalog as the current catalog of the session
        tableEnv.useCatalog("mypg");

        Arrays.stream(tableEnv.listDatabases()).forEach(System.out::println);

        Arrays.stream(tableEnv.listTables()).forEach(System.out::println);

        tableEnv.executeSql("select * from `public.primitive_arr_table`").print();
//        postgres
//mydb
//bang.primitive_table
//public.primitive_arr_table
//public.primitive_serial_table
//public.primitive_table
//public.primitive_table2
//public.simple_t1
//+----------+-----------+--------------------------------+-----------+-----------+-----------------+----------------------+-----------------------------+--------------------------------+----------------------+---------------------+-----------+-----------+-----------------+-----------------------+--------------------------------+--------------------------+----------------------+
//| row_kind |   int_arr |                      bytea_arr | short_arr |  long_arr |        real_arr | double_precision_arr |                 numeric_arr |            numeric_arr_default |          decimal_arr |         boolean_arr |  text_arr |  char_arr |   character_arr | character_varying_arr |                  timestamp_arr |                 date_arr |             time_arr |
//+----------+-----------+--------------------------------+-----------+-----------+-----------------+----------------------+-----------------------------+--------------------------------+----------------------+---------------------+-----------+-----------+-----------------+-----------------------+--------------------------------+--------------------------+----------------------+
//|       +I | [1, 2, 3] |  [[92, 120, 51, 50], [92, 1... | [3, 4, 5] | [4, 5, 6] | [5.5, 6.6, 7.7] |      [6.6, 7.7, 8.8] | [7.70000, 8.80000, 9.90000] |  [8.800000000000000000, 9.9... | [9.90, 10.10, 11.11] | [true, false, true] | [a, b, c] | [b, c, d] | [b  , c  , d  ] |             [b, c, d] |  [2016-06-22T19:10:25, 2019... | [2015-01-01, 2020-01-01] | [00:51:03, 00:59:03] |
//+----------+-----------+--------------------------------+-----------+-----------+-----------------+----------------------+-----------------------------+--------------------------------+----------------------+---------------------+-----------+-----------+-----------------+-----------------------+--------------------------------+--------------------------+----------------------+
//1 row in set
//
//Process finished with exit code 0
    }
}
