/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.TaskManagerOptions.TaskManagerLoadBalanceMode;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadingUtilization;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Simple {@link SlotAssigner} that treats all slots and slot sharing groups equally. */
public class DefaultSlotAssigner extends SlotAssigner {

    @VisibleForTesting
    public DefaultSlotAssigner() {
        this(TaskManagerLoadBalanceMode.NONE);
    }

    public DefaultSlotAssigner(TaskManagerLoadBalanceMode loadBalanceMode) {
        super(loadBalanceMode);
    }

    @Override
    public Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism,
            TaskExecutorsLoadingUtilization taskExecutorsLoadingUtilization,
            JobAllocationsInformation previousAllocations) {
        List<ExecutionSlotSharingGroup> allGroups =
                slotSharingStrategy.getExecutionSlotSharingGroups(
                        jobInformation, vertexParallelism);
        return requestSlotMatcher.matchRequestsWithSlots(
                allGroups,
                selectSlotsInMinimalTaskExecutors(freeSlots, allGroups, Collections.emptyList()),
                taskExecutorsLoadingUtilization);
    }

    @Override
    public List<TaskManagerLocation> sortPrioritizedTaskExecutors(
            Collection<? extends SlotInfo> slots, Collection<AllocationScore> scores) {
        Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsByTaskExecutor =
                SlotAssigner.getSlotsPerTaskExecutor(slots);
        return slotsByTaskExecutor.keySet().stream()
                .sorted(
                        (left, right) ->
                                Integer.compare(
                                        slotsByTaskExecutor.get(right).size(),
                                        slotsByTaskExecutor.get(left).size()))
                .collect(Collectors.toList());
    }
}
