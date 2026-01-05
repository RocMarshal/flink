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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismInfo;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismStore;
import org.apache.flink.runtime.scheduler.MutableVertexParallelismStore;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.POINTWISE;
import static org.apache.flink.runtime.scheduler.adaptive.ParallelismBasedForwardEdgesAdapter.adaptiveChangeForwardJobEdges;
import static org.apache.flink.runtime.scheduler.adaptive.ParallelismBasedForwardEdgesAdapter.isSameParallelism;
import static org.assertj.core.api.Assertions.assertThat;

/** The test for {@link ParallelismBasedForwardEdgesAdapter}. */
class ParallelismBasedForwardEdgesAdapterTest {

    @Test
    void testAdaptiveChangeForwardJobEdges() throws Exception {
        final JobGraph jobGraph = generateJobGraph();

        // Test initial info.
        JobGraph adjustedJobGraph = InstantiationUtil.clone(jobGraph);
        MutableVertexParallelismStore vertexParallelismStore =
                getParallelismStore(adjustedJobGraph, 4, 4, 4, 4);
        adaptiveChangeForwardJobEdges(adjustedJobGraph, vertexParallelismStore);
        assertInitialForwardableInputs(adjustedJobGraph, vertexParallelismStore);

        // Test forward -> rebalance.
        adjustedJobGraph = InstantiationUtil.clone(jobGraph);
        vertexParallelismStore = getParallelismStore(adjustedJobGraph, 4, 4, 2, 4);
        adaptiveChangeForwardJobEdges(adjustedJobGraph, vertexParallelismStore);
        assertInitialForwardableInputs(adjustedJobGraph, vertexParallelismStore);
    }

    private static MutableVertexParallelismStore getParallelismStore(
            JobGraph adjustedJobGraph,
            int... parallelismsOfVerticesSortedTopologicallyFromSources) {
        MutableVertexParallelismStore vertexParallelismStore = new DefaultVertexParallelismStore();
        List<JobVertex> verticesSortedTopologicallyFromSources =
                adjustedJobGraph.getVerticesSortedTopologicallyFromSources();
        for (int i = 0; i < verticesSortedTopologicallyFromSources.size(); i++) {
            vertexParallelismStore.setParallelismInfo(
                    verticesSortedTopologicallyFromSources.get(i).getID(),
                    new DefaultVertexParallelismInfo(
                            parallelismsOfVerticesSortedTopologicallyFromSources[i],
                            Integer.MAX_VALUE,
                            integer -> Optional.empty()));
        }
        return vertexParallelismStore;
    }

    private static JobGraph generateJobGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Long> mappedStream1 =
                env.fromSource(
                                new DataGeneratorSource<>(
                                        index -> index,
                                        Long.MAX_VALUE,
                                        RateLimiterStrategy.perSecond(1),
                                        Types.LONG),
                                WatermarkStrategy.noWatermarks(),
                                "GenSource1")
                        .map(ignored -> ignored)
                        .name("map1");

        SingleOutputStreamOperator<Long> mappedStream2 = mappedStream1.map(s -> s).name("map2");
        mappedStream1.forward().map(ignored -> ignored).name("map3").disableChaining();
        mappedStream2.forward().map(ignored -> ignored).name("map4").disableChaining();
        mappedStream2.forward().map(ignored -> ignored).name("map5").disableChaining();

        return env.getStreamGraph().getJobGraph();
    }

    private static void assertInitialForwardableInputs(
            JobGraph adjustedJobGraph, MutableVertexParallelismStore vertexParallelismStore) {
        for (JobVertex vertex : adjustedJobGraph.getVerticesSortedTopologicallyFromSources()) {
            for (JobEdge input : vertex.getInputs()) {
                if (input.isInitialForward()) {
                    final boolean forwardable = isSameParallelism(input, vertexParallelismStore);
                    StreamPartitioner<?> partitioner =
                            forwardable ? new ForwardPartitioner<>() : new RebalancePartitioner<>();
                    assertThat(input.isInitialForward()).isTrue();
                    assertThat(input.isForward()).isEqualTo(forwardable);
                    assertThat(input.getDistributionPattern())
                            .isEqualTo(forwardable ? POINTWISE : ALL_TO_ALL);
                    assertThat(input.getUpstreamSubtaskStateMapper())
                            .isEqualTo(partitioner.getUpstreamSubtaskStateMapper());
                    assertThat(input.getDownstreamSubtaskStateMapper())
                            .isEqualTo(partitioner.getDownstreamSubtaskStateMapper());
                    assertThat(input.getShipStrategyName()).isEqualTo(partitioner.toString());

                    IntermediateDataSet source = input.getSource();
                    assertThat(source.isInitialForward()).isTrue();
                    assertThat(source.getDistributionPattern())
                            .isEqualTo(forwardable ? POINTWISE : ALL_TO_ALL);
                    assertThat(source.isForward()).isEqualTo(forwardable);
                    assertThat(input.getSource())
                            .isEqualTo(
                                    input.getSource()
                                            .getProducer()
                                            .getIntermediateDataSet(input.getSourceId()));
                }
            }
        }
    }
}
