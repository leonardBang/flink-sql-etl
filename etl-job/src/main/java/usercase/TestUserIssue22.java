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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.lit;


import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.or;

public class TestUserIssue22 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
            new Order(1L, "beer", 3, 1505529000L), //2017-09-16 10:30:00
            new Order(1L, "beer", 3, 1505529000L), //2017-09-16 10:30:00
            new Order(3L, "rubber", 2,1505527800L),//2017-09-16 10:10:00
            new Order(3L, "rubber", 2,1505527800L),//2017-09-16 10:10:00
            new Order(1L, "diaper", 4,1505528400L),//2017-09-16 10:20:00
            new Order(1L, "diaper", 4,1505528400L)//2017-09-16 10:20:00
        ));

        // 老版本
//        Table orders = tEnv.fromDataStream(orderA.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Order>() {
//                @Override
//                public long extractAscendingTimestamp(Order element) {
//                    return element.rowtime;
//                }
//            }), $("user"), $("product"), $("amount"),$("rowtime").rowtime());
        // 新版本
        Table orders = tEnv.fromDataStream(orderA.assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator((ctx) -> new WatermarkGenerator<Order>() {
                @Override
                public void onEvent(Order order, long eventTimestamp, WatermarkOutput watermarkOutput) {
                    watermarkOutput.emitWatermark(new Watermark(eventTimestamp));
                }
                @Override
                public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                }
            }))
            , $("user"), $("product"), $("amount"),$("rowtime").rowtime());
        System.out.println(orders.getSchema());


        Table result = orders
            .window(Tumble.over(lit(5).minutes())
                .on($("rowtime")).as("w"))
            .groupBy($("w"), $("product"),$("user"))// group by key and window
            // access window properties and aggregate
            .select(
                $("user"),
                $("w").start(),
                $("w").end(),
                $("w").rowtime(),
                $("amount").sum().as("d")
            );



        tEnv.toRetractStream(result, Row.class).print();


        env.execute();
    }

    /**
     * Simple POJO.
     */
    public static class Order {
        public Long user;
        public String product;
        public int amount;
        public Long rowtime;

        public Order() {
        }

        public Order(Long user, String product, int amount, Long rowtime) {
            this.user = user;
            this.product = product;
            this.amount = amount;
            this.rowtime = rowtime;
        }

        @Override
        public String toString() {
            return "Order{" +
                "user=" + user +
                ", product='" + product + '\'' +
                ", amount=" + amount +
                ", ts=" + rowtime +
                '}';
        }
    }
}
