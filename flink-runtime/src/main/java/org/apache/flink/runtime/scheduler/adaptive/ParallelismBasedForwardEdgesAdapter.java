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

import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.POINTWISE;

/** The utils to try automatic adapt the setting of forward type job edges. */
class ParallelismBasedForwardEdgesAdapter {

    private ParallelismBasedForwardEdgesAdapter() {}

    /**
     * Update the connected setting when switching between FORWARD and REBALANCE for {@link
     * AdaptiveScheduler}.
     */
    static void adaptiveChangeForwardJobEdges(
            JobGraph jobGraph, VertexParallelismStore vertexParallelismStore) {
        for (JobVertex vertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
            for (JobEdge input : vertex.getInputs()) {
                if (input.isInitialForward()) {
                    Preconditions.checkState(
                            input.isInitialForward() && input.getSource().isInitialForward(),
                            "Unexpected chained result occurred to here, may be a bug.");
                    final boolean forwardable = isSameParallelism(input, vertexParallelismStore);
                    StreamPartitioner<?> partitioner =
                            forwardable ? new ForwardPartitioner<>() : new RebalancePartitioner<>();
                    input.forceUpdate(
                            forwardable,
                            forwardable ? POINTWISE : ALL_TO_ALL,
                            partitioner.getUpstreamSubtaskStateMapper(),
                            partitioner.getDownstreamSubtaskStateMapper(),
                            partitioner.toString());
                }
            }
        }
    }

    static boolean isSameParallelism(JobEdge jobEdge, VertexParallelismStore vertexParallelism) {
        final JobVertexID upstreamVertexID = jobEdge.getSource().getProducer().getID();
        final JobVertexID downstreamVertexID = jobEdge.getTarget().getID();
        int upParallelism = vertexParallelism.getParallelismInfo(upstreamVertexID).getParallelism();
        int downParallelism =
                vertexParallelism.getParallelismInfo(downstreamVertexID).getParallelism();
        return upParallelism == downParallelism;
    }
}
