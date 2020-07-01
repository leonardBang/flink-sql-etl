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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.Instant;

public class TestUserIssue9 {
    public static void main(String[] args) throws Exception {
        //2020-06-29 21:12:04.471
        //2020-06-29 23:07:01.1245406
        System.out.println(Timestamp.from(Instant.ofEpochMilli( 1593443236124L)));
//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(environment);
//        tEnv.connect(new Kafka()
//            .version("0.10")
//            .topic("jes_topic_evtime")
//            .property("zookeeper.connect", "localhost:2181")
//            .property("bootstrap.servers", "localhost:9092")
//            .property("group.id", "grp1")
//            .startFromEarliest()
//        ).withFormat(new Json()
//            .failOnMissingField(false).deriveSchema())
//            .withSchema(new Schema()
//                .field("acct", "STRING")
//                .field("evtime", "LONG")
//                .field("logictime","TIMESTAMP(3)")
//                .rowtime(new Rowtime().timestampsFromField("evtime").watermarksPeriodicBounded(5000)))
//            .inAppendMode().createTemporaryTable("testTableName");
//
//
//
//        Table testTab = tEnv.sqlQuery("SELECT acct, evtime, logictime FROM testTableName")
//            .window(Tumble.over("5.seconds").on("logictime").as("w1"))
//            .groupBy("w1, acct")
//            .select("w1.rowtime, acctno");
//
//        tEnv.toRetractStream(testTab, Row.class).print();
//        environment.execute();
    }
}
