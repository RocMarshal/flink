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
package fug.state;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("taskmanager.numberOfTaskSlots", "8");
        //        conf.setString("state.checkpoint-storage", "filesystem");
        conf.setString("state.backend", "rocksdb");
        conf.setString("state.backend.changelog.periodic-materialize.interval", "30s");
        //        conf.setString("cluster.evenly-spread-out-slots", "true");
        conf.setString("state.backend.changelog.enabled", "true");
        conf.setString("state.backend.changelog.storage", "filesystem");
        conf.setString(
                "dstl.dfs.base-path", "hdfs://localhost:9000/flink-optimize-delete-changelog");
        conf.setString("dstl.dfs.batch.persist-size-threshold", "1kb");
        //        conf.setString("state.backend", "filesystem");
        conf.setString("state.backend.incremental", "true");
        conf.setString("state.savepoints.dir", "hdfs://localhost:9000/flink-optimize-delete-test");

        conf.setString("state.checkpoints.dir", "hdfs://localhost:9000/flink-optimize-delete-test");
        conf.setString("execution.checkpointing.interval", "10s");
        conf.setString("state.checkpoints.num-retained", "1");

        //        conf.setString("execution.savepoint.path",
        // "hdfs://localhost:9000/flink-optimize-delete-test/aa52de6a410a4af023de9664cb129a2c/chk-3"); // Anchor-A:

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(2);

        DataStream<Tuple2<Integer, Integer>> rebalance =
                env.addSource(
                                new RichParallelSourceFunction<Tuple2<Integer, Integer>>() {

                                    AtomicBoolean running = new AtomicBoolean(true);
                                    Random random = new Random();

                                    @Override
                                    public void run(
                                            SourceContext<Tuple2<Integer, Integer>> sourceContext)
                                            throws Exception {
                                        while (running.get()) {
                                            sourceContext.collect(
                                                    Tuple2.of(random.nextInt(100), 1));
                                            Thread.sleep(Duration.ofMillis(1).toMillis());
                                        }
                                    }

                                    @Override
                                    public void cancel() {
                                        running.set(false);
                                    }

                                    @Override
                                    public void close() {
                                        running.set(false);
                                    }
                                })
                        .name("S")
                        .setParallelism(2)
                        .forward() /*.rebalance()*/;
        /*
                rebalance.addSink(new DiscardingSink<>()).name("SKOne").setParallelism(2);
        */
        // rebalance.addSink(new DiscardingSink<>()).name("SKTwo").setParallelism(2);
        rebalance
                .keyBy(
                        new KeySelector<Tuple2<Integer, Integer>, Integer>() {
                            @Override
                            public Integer getKey(Tuple2<Integer, Integer> value) throws Exception {
                                return value.f0;
                            }
                        })
                .sum("f0")
                .setParallelism(1)
                .print();

        env.execute(Main.class.getSimpleName());
    }
}
