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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.adaptive.allocator.StateLocalitySlotAssigner.AllocationScore;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

/**
 * The slots candidates filter to keep the slots candidates based on the minimum task executors
 * rule.
 */
class MinimalTaskExecutorSlotsFilter implements SlotsFilter {

    private final boolean localityPreferred;

    public MinimalTaskExecutorSlotsFilter(boolean localityPreferred) {
        this.localityPreferred = localityPreferred;
    }

    @Override
    public Collection<? extends SlotInfo> filterSlots(
            Collection<? extends SlotInfo> slots,
            Collection<ExecutionSlotSharingGroup> groups,
            Collection<AllocationScore> scores) {
        int redundantSlots = slots.size() - groups.size();
        if (redundantSlots <= 0) {
            return slots;
        }

        Map<ResourceID, ? extends Set<? extends SlotInfo>> slotsByTaskExecutor =
                getSlotsPerTaskExecutor(slots);

        List<ResourceID> orderedTaskExecutors;
        if (localityPreferred) {
            // We must first ensure the minimum number of TMs,
            // and then prioritize selecting the remaining target TMs slots by locality.
            orderedTaskExecutors =
                    getLocalityPreferredOrderedTaskExecutors(slots, scores, slotsByTaskExecutor);
        } else {
            orderedTaskExecutors = getSlotNumOrderedTaskExecutors(slotsByTaskExecutor);
        }

        Set<SlotInfo> slotSet = new HashSet<>(slots);
        for (ResourceID resourceID : orderedTaskExecutors) {
            Set<? extends SlotInfo> slotInfos = slotsByTaskExecutor.get(resourceID);
            if (redundantSlots >= slotInfos.size()) {
                redundantSlots -= slotInfos.size();
                slotSet.removeAll(slotInfos);
            } else {
                break;
            }
        }
        return slotSet;
    }

    private static Map<ResourceID, ? extends Set<? extends SlotInfo>> getSlotsPerTaskExecutor(
            Collection<? extends SlotInfo> slots) {
        return slots.stream()
                .collect(
                        Collectors.groupingBy(
                                slotInfo -> slotInfo.getTaskManagerLocation().getResourceID(),
                                Collectors.mapping(identity(), Collectors.toSet())));
    }

    private static List<ResourceID> getSlotNumOrderedTaskExecutors(
            Map<ResourceID, ? extends Set<? extends SlotInfo>> slotsByTaskExecutor) {
        return slotsByTaskExecutor.keySet().stream()
                .sorted(
                        (left, right) ->
                                Integer.compare(
                                        slotsByTaskExecutor.get(right).size(),
                                        slotsByTaskExecutor.get(left).size()))
                .collect(Collectors.toList());
    }

    /**
     * Get task executors in the special ascending order, which is sorted by the number of slots and
     * the summary allocation scores.
     */
    private static List<ResourceID> getLocalityPreferredOrderedTaskExecutors(
            Collection<? extends SlotInfo> freeSlots,
            Collection<AllocationScore> scores,
            Map<ResourceID, ? extends Set<? extends SlotInfo>> slotsByTaskExecutor) {

        final Map<AllocationID, ResourceID> allocationIdToResourceId =
                freeSlots.stream()
                        .collect(
                                Collectors.toMap(
                                        SlotInfo::getAllocationId,
                                        slotInfo ->
                                                slotInfo.getTaskManagerLocation().getResourceID()));

        Map<ResourceID, Long> resourceScores = new HashMap<>(slotsByTaskExecutor.size());
        for (AllocationScore allocScore : scores) {
            final ResourceID resourceID =
                    allocationIdToResourceId.get(allocScore.getAllocationId());
            if (Objects.nonNull(resourceID)) {
                resourceScores.compute(
                        resourceID,
                        (rid, oldVal) ->
                                Objects.isNull(oldVal)
                                        ? allocScore.getScore()
                                        : oldVal + allocScore.getScore());
            }
        }
        return slotsByTaskExecutor.keySet().stream()
                .sorted(
                        (left, right) -> {
                            int diff =
                                    slotsByTaskExecutor.get(left).size()
                                            - slotsByTaskExecutor.get(right).size();
                            if (diff == 0) {
                                return Long.compare(
                                        resourceScores.getOrDefault(left, 0L),
                                        resourceScores.getOrDefault(right, 0L));
                            }
                            return diff > 0 ? 1 : -1;
                        })
                .collect(Collectors.toList());
    }
}
