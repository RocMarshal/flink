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

package org.apache.flink.streaming.examples;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/** Debug for adaptive scheduler. */
public class AdaptiveScheduler {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.MINI_CLUSTER_NUM_TASK_MANAGERS, 4);
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 2);
        conf.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);

        env.addSource(
                        new RichParallelSourceFunction<String>() {
                            volatile boolean running = true;

                            @Override
                            public void run(SourceContext<String> ctx) throws Exception {
                                while (running) {}
                            }

                            @Override
                            public void cancel() {
                                running = false;
                            }
                        })
                .setParallelism(2)
                .slotSharingGroup("ssgA")
                .sinkTo(new DiscardingSink<>())
                .slotSharingGroup("ssgB");

        env.execute();
    }
}
