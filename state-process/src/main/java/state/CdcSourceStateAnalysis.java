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

package state;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;

import java.nio.charset.StandardCharsets;

public class CdcSourceStateAnalysis {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
        bEnv.setParallelism(1);

        Configuration configuration = new Configuration();
        configuration.setString(CheckpointingOptions.STATE_BACKEND.key(), "com.alibaba.flink.statebackend.GeminiStateBackendFactory");
        ExistingSavepoint savepoint = Savepoint.load(bEnv, "/Users/bang/flink-cdc-debug",
            StateBackendLoader.loadStateBackendFromConfig(configuration, Thread.currentThread().getContextClassLoader(), null));


        DataSet<byte[]> offsetStat = savepoint.readUnionState("6cdc5bb954874d922eaee11a8e7b5dd5", "offset-states", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        System.out.println(new String(offsetStat.collect().get(0), StandardCharsets.UTF_8) );
        DataSet<String> historyRecords = savepoint.readUnionState("6cdc5bb954874d922eaee11a8e7b5dd5", "history-records-states", BasicTypeInfo.STRING_TYPE_INFO);
        historyRecords.print();

        bEnv.execute("");
    }
}
