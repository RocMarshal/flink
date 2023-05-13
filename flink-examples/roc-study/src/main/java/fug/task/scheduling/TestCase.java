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

package fug.task.scheduling;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestCase {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
                        .setParallelism(2)
                        .name("A")
                        .rebalance();
        rebalance
                .map(
                        new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                            @Override
                            public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value)
                                    throws Exception {
                                return value;
                            }
                        })
                .name("B")
                .setParallelism(3)
                .rebalance()
                .map(
                        new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                            @Override
                            public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value)
                                    throws Exception {
                                return value;
                            }
                        })
                .slotSharingGroup("aaa")
                .name("C")
                .setParallelism(5)
                .rebalance()
                .map(
                        new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                            @Override
                            public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value)
                                    throws Exception {
                                return value;
                            }
                        })
                .name("D")
                .setParallelism(10);

        env.execute(TestCase.class.getSimpleName());
    }
}
