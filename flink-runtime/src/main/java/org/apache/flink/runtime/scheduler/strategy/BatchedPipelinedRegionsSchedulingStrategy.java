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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.scheduler.SchedulerOperations;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SchedulingStrategy} instance which schedules tasks of all required regions by batch in one
 * operation.
 */
public class BatchedPipelinedRegionsSchedulingStrategy extends PipelinedRegionSchedulingStrategy {

    public BatchedPipelinedRegionsSchedulingStrategy(
            final SchedulerOperations schedulerOperations,
            final SchedulingTopology schedulingTopology) {
        super(schedulerOperations, schedulingTopology);
    }

    @Override
    protected void maybeScheduleRegions(Set<SchedulingPipelinedRegion> regions) {
        final Set<SchedulingPipelinedRegion> regionsToSchedule = new HashSet<>();
        Set<SchedulingPipelinedRegion> nextRegions = regions;
        while (!nextRegions.isEmpty()) {
            nextRegions = addSchedulableAndGetNextRegions(nextRegions, regionsToSchedule);
        }
        // schedule regions in topological order.
        List<SchedulingPipelinedRegion> schedulingPipelinedRegions =
                SchedulingStrategyUtils.sortPipelinedRegionsInTopologicalOrder(
                        schedulingTopology, regionsToSchedule);
        List<ExecutionVertexID> executionVertices = new ArrayList<>();
        schedulingPipelinedRegions.forEach(
                rg -> {
                    checkState(
                            areRegionVerticesAllInCreatedState(rg),
                            "BUG: trying to schedule a region which is not in CREATED state");
                    executionVertices.addAll(regionVerticesSorted.get(rg));
                });
        scheduledRegions.addAll(schedulingPipelinedRegions);
        schedulerOperations.allocateSlotsAndDeploy(executionVertices);
    }

    /** The factory for creating {@link BatchedPipelinedRegionsSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {
        @Override
        public SchedulingStrategy createInstance(
                final SchedulerOperations schedulerOperations,
                final SchedulingTopology schedulingTopology) {
            return new BatchedPipelinedRegionsSchedulingStrategy(
                    schedulerOperations, schedulingTopology);
        }
    }
}
