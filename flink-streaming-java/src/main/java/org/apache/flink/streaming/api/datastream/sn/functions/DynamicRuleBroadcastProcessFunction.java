/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream.sn.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.sn.rules.Metric;
import org.apache.flink.streaming.api.datastream.sn.rules.Rule;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/** Dynamic Rule Function based on {@link org.apache.flink.streaming.api.datastream.BroadcastConnectedStream}. */
public class DynamicRuleBroadcastProcessFunction
        extends BroadcastProcessFunction<Metric, Rule, Object> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void processElement(
            Metric value,
            BroadcastProcessFunction<Metric, Rule, Object>.ReadOnlyContext ctx,
            Collector<Object> out) throws Exception {

    }

    @Override
    public void processBroadcastElement(
            Rule value,
            BroadcastProcessFunction<Metric, Rule, Object>.Context ctx,
            Collector<Object> out) throws Exception {

    }
}
