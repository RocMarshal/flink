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
import org.apache.flink.runtime.scheduler.adaptive.allocator.StateLocalitySlotAssigner.AllocationScore;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.state.heap.HeapPriorityQueue;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.jobmaster.slotpool.TasksBalancedRequestSlotMatchingStrategy.SlotElementPriorityComparator;
import static org.apache.flink.runtime.jobmaster.slotpool.TasksBalancedRequestSlotMatchingStrategy.SlotInfoElement;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.StateLocalitySlotAssigner.ScoreKey;

/** The balanced request slot matching matcher implementation. */
public class TaskBalancedRequestSlotMatcher implements RequestSlotMatcher {

    @Override
    public Collection<SlotAssignment> matchRequestsWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<? extends SlotInfo> freeSlots,
            TaskExecutorsLoadingUtilization taskExecutorsLoadingUtilization,
            @Nonnull Queue<AllocationScore> scores) {
        final List<SlotAssignment> slotAssignments = new ArrayList<>(requestGroups.size());
        Map<ResourceID, LoadingWeight> taskExecutorsLoad =
                taskExecutorsLoadingUtilization.getTaskExecutorsLoadingWeight();
        HeapPriorityQueue<SlotInfoElement<SlotInfo>> slotElementPriorityQueue = new HeapPriorityQueue<>(
                new SlotElementPriorityComparator<>(taskExecutorsLoad), freeSlots.size());
        freeSlots.stream()
                        .map(
                                (Function<SlotInfo, SlotInfoElement<SlotInfo>>)
                                        SlotInfoElement::new).forEach(slotElementPriorityQueue::add);


        if (scores.isEmpty()) {
            for (ExecutionSlotSharingGroup requestGroup : requestGroups) {}

        } else {
            Map<ScoreKey, Long> scoreMap = getScoreMap(scores);
            for (ExecutionSlotSharingGroup requestGroup : requestGroups) {}
        }
        return slotAssignments;
    }

    private static Map<ScoreKey, Long> getScoreMap(Queue<AllocationScore> scores) {
        return scores.stream()
                .collect(Collectors.toMap(ScoreKey::new, allocationScore -> allocationScore.score));
    }
}
