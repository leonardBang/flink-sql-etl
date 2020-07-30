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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.sql.Date;
import java.time.LocalDate;

public class TestUserIssue17 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        environment.setParallelism(1);


        tableEnvironment.executeSql("CREATE TABLE orders (\n" +
            "  order_number INT,\n" +
            "  order_date INT,\n" +
            "  purchaser INT,\n" +
            "  quantity INT,\n" +
            "  product_id INT\n" +
            " ) WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = 'dbserver1.inventory.orders',\n" +
            "  'scan.startup.mode' = 'earliest-offset',\n" +
            "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
            "  'properties.group.id' = 'xxtestgroup1',\n" +
            "  'format' = 'debezium-json',\n" +
            "  'debezium-json.schema-include' = 'true' " +
            " )");
        tableEnvironment.executeSql("create table orders1 ( " +
            "  order_number INT,\n" +
            "  order_date date,\n" +
            "  purchaser INT,\n" +
            "  quantity INT,\n" +
            "  product_id INT," +
            "  PRIMARY KEY(order_number) NOT ENFORCED\n" +
            ") with ( " +
            "  'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://localhost:3306/inventory',\n" +
            "    'username' = 'mysqluser',\n" +
            "    'password' = 'mysqlpw',\n" +
            "    'table-name' = 'orders2',\n" +
            "    'driver' = 'com.mysql.jdbc.Driver')");
        tableEnvironment.registerFunction("int2Date", new Int2DateFunc());
       tableEnvironment.executeSql("insert into orders1 SELECT  order_number, int2Date(order_date),purchaser,quantity,product_id from orders ").getJobClient().get()
       .getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
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
