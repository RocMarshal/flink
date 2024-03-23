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
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/** AdaptiveSchedulerDemo class. */
public class AdaptiveSchedulerDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //        conf.setString("taskmanager.numberOfTaskSlots", "100");

        conf.setString("taskmanager.numberOfTaskSlots", "2");
        conf.setString("local.number-taskmanager", "6");
        conf.setString("rest.flamegraph.enabled", "true");

        //        conf.setString("jobmanager.scheduler", "adaptive");
        conf.setString("taskmanager.load-balance.mode", "TASKS");

        //        conf.setString("job.autoscaler.enabled", "true");
        //        conf.setString("job.autoscaler.scaling.enabled", "true");
        //        conf.setString("job.autoscaler.stabilization.interval", "1m");
        //        conf.setString("job.autoscaler.metrics.window", "2m");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(5);

        env.addSource(new SourceFunction<Long>() {
            volatile boolean running = true;
                    @Override
                    public void run(SourceContext<Long> ctx) throws Exception {
                        while (running) {
                            Thread.sleep(1000L);
                            ctx.collect(new Random().nextLong());
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .rebalance()
                .map((MapFunction<Long, String>) String::valueOf)
                .name("RateLimiterMapFunction")
                .rebalance()
                .addSink(new DiscardingSink<>())
                .name("MySink");

        env.execute(AdaptiveSchedulerDemo.class.getSimpleName());
    }
}
