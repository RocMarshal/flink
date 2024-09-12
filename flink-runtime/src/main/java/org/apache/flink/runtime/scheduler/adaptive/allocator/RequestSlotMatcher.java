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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadingUtilization;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.adaptive.allocator.AllocationScore.ScoreKey;

/**
 * The interface to define the matcher how to matching the target physical slots with the execution
 * slot sharing groups.
 */
public interface RequestSlotMatcher {

    Supplier<FlinkRuntimeException> NO_SUITABLE_SLOTS_EXCEPTION_GETTER =
            () -> new FlinkRuntimeException("No suitable slots enough.");

    /**
     * Match slots from the free slots with the given collection of requests execution groups.
     *
     * @param requestGroups the requested execution slot sharing groups.
     * @param freeSlots the available free slots.
     * @param taskExecutorsLoadingUtilization the task executors loading and slot utilization.
     * @param scores the allocation scores.
     * @return The assignment result.
     */
    Collection<SlotAssignment> matchRequestsWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<? extends SlotInfo> freeSlots,
            TaskExecutorsLoadingUtilization taskExecutorsLoadingUtilization,
            @Nonnull Queue<AllocationScore> scores);

    /**
     * Match slots from the free slots with the given collection of requests execution groups.
     *
     * @param requestGroups the requested execution slot sharing groups.
     * @param freeSlots the available free slots.
     * @param taskExecutorsLoadingUtilization the task executors loading and slot utilization.
     * @return The assignment result.
     */
    default Collection<SlotAssignment> matchRequestsWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<? extends SlotInfo> freeSlots,
            TaskExecutorsLoadingUtilization taskExecutorsLoadingUtilization) {
        return matchRequestsWithSlots(
                requestGroups, freeSlots, taskExecutorsLoadingUtilization, new PriorityQueue<>());
    }

    /**
     * Helper class to represent the slot and the loading or slots utilization weight info of the
     * task executor where the slot is located at.
     */
    class SlotTaskExecutorWeight<T> {
        final @Nonnull T taskExecutorWeight;
        final @Nonnull SlotInfo slotInfo;

        SlotTaskExecutorWeight(@Nonnull T taskExecutorWeight, @Nonnull SlotInfo slotInfo) {
            this.taskExecutorWeight = taskExecutorWeight;
            this.slotInfo = slotInfo;
        }

        ResourceID getResourceID() {
            return slotInfo.getTaskManagerLocation().getResourceID();
        }
    }

    static Map<ScoreKey, AllocationScore> getScoreMap(Queue<AllocationScore> scores) {
        return scores.stream()
                .collect(Collectors.toMap(AllocationScore::getScoreKey, Function.identity()));
    }

    static Map<ResourceID, Set<SlotInfo>> getSlotsPerTaskExecutor(
            Collection<? extends SlotInfo> freeSlots) {
        return freeSlots.stream()
                .collect(
                        Collectors.groupingBy(
                                slot -> slot.getTaskManagerLocation().getResourceID(),
                                Collectors.toSet()));
    }
}
