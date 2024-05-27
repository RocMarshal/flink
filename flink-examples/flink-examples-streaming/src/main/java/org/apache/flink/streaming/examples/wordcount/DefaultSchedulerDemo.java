/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

/** DefaultSchedulerDemo class. */
public class DefaultSchedulerDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //        conf.setString("taskmanager.numberOfTaskSlots", "100");

        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 2);
        conf.set(TaskManagerOptions.MINI_CLUSTER_NUM_TASK_MANAGERS, 3);
        conf.set(RestOptions.ENABLE_FLAMEGRAPH, true);
        conf.set(
                TaskManagerOptions.TASK_MANAGER_LOAD_BALANCE_MODE,
                TaskManagerOptions.TaskManagerLoadBalanceMode.TASKS);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(5);

        GeneratorFunction<Long, Long> generatorFunction = index -> index;
        double recordsPerSecond = 100;

        DataGeneratorSource<Long> source =
                new DataGeneratorSource<>(
                        generatorFunction,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(recordsPerSecond),
                        Types.LONG);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "generateSource")
                .setParallelism(2)
                .rebalance()
                .map((MapFunction<Long, String>) String::valueOf)
                .setParallelism(3)
                .name("RateLimiterMapFunction")
                .rebalance()
                .addSink(new DiscardingSink<>())
                .setParallelism(5)
                .name("MySink");

        env.execute(DefaultSchedulerDemo.class.getSimpleName());
    }
}
