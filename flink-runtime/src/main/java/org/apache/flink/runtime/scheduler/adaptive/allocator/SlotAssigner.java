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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.TaskManagerOptions.TaskManagerLoadBalanceMode;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadingUtilization;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;

/** The abstract class for assigning slots to slot sharing groups. */
@Internal
public abstract class SlotAssigner {

    protected final TaskManagerLoadBalanceMode loadBalanceMode;
    protected final SlotSharingStrategy slotSharingStrategy;
    protected final RequestSlotMatcher requestSlotMatcher;

    public SlotAssigner(TaskManagerLoadBalanceMode loadBalanceMode) {
        this.loadBalanceMode = Preconditions.checkNotNull(loadBalanceMode);
        this.slotSharingStrategy = getSlotSharingStrategy();
        this.requestSlotMatcher = getRequestSlotMatcher();
    }

    private SlotSharingStrategy getSlotSharingStrategy() {
        return loadBalanceMode == TaskManagerLoadBalanceMode.TASKS
                ? TaskBalancedSlotSharingStrategy.INSTANCE
                : DefaultSlotSharingStrategy.INSTANCE;
    }

    private RequestSlotMatcher getRequestSlotMatcher() {
        switch (loadBalanceMode) {
            case TASKS:
                return new TaskBalancedRequestSlotMatcher();
            case SLOTS:
                return new EvenlySpreadOutRequestSlotMatcher();
            case NONE:
            default:
                return new DefaultRequestSlotMatcher();
        }
    }

    public abstract Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism,
            TaskExecutorsLoadingUtilization taskExecutorsLoadingUtilization,
            JobAllocationsInformation previousAllocations);

    /**
     * Select the target slots to assign with the requested groups.
     *
     * @param slots the raw slots to filter.
     * @param groups the request execution slot sharing groups.
     * @param scores the allocation scores.
     * @return the target slots that are distributed on the minimal task executors.
     */
    Collection<? extends SlotInfo> selectSlotsInMinimalTaskExecutors(
            Collection<? extends SlotInfo> slots,
            Collection<ExecutionSlotSharingGroup> groups,
            Collection<AllocationScore> scores) {
        if (slots.size() - groups.size() <= 0) {
            return slots;
        }

        List<TaskManagerLocation> orderedTaskExecutors =
                sortPrioritizedTaskExecutors(slots, scores);
        Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsByTaskExecutor =
                SlotAssigner.getSlotsPerTaskExecutor(slots);

        int requestedSlots = groups.size();
        final List<SlotInfo> result = new ArrayList<>();
        for (TaskManagerLocation tml : orderedTaskExecutors) {
            if (requestedSlots <= 0) {
                break;
            }
            final Set<? extends SlotInfo> slotInfos = slotsByTaskExecutor.get(tml);
            requestedSlots -= slotInfos.size();
            result.addAll(slotInfos);
        }
        return result;
    }

    /**
     * Get the task executors with the order that aims to priority assigning requested groups on it.
     *
     * @param slots the all slots.
     * @param scores the allocation scores.
     * @return the task executors with the order that aims to priority assigning requested groups on
     *     it.
     */
    protected abstract List<TaskManagerLocation> sortPrioritizedTaskExecutors(
            Collection<? extends SlotInfo> slots, Collection<AllocationScore> scores);

    static Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> getSlotsPerTaskExecutor(
            Collection<? extends SlotInfo> slots) {
        return slots.stream()
                .collect(
                        Collectors.groupingBy(
                                SlotInfo::getTaskManagerLocation,
                                Collectors.mapping(identity(), Collectors.toSet())));
    }
}
