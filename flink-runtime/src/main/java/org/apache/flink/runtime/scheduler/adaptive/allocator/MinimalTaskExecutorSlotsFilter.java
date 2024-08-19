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
enum MinimalTaskExecutorSlotsFilter implements SlotsFilter {
    INSTANCE;

    @Override
    public Collection<? extends SlotInfo> filterSlots(
            Collection<? extends SlotInfo> rawFreeSlots,
            Collection<ExecutionSlotSharingGroup> groups,
            Collection<AllocationScore> scores) {
        int redundantSlots = rawFreeSlots.size() - groups.size();
        if (redundantSlots <= 0) {
            return rawFreeSlots;
        }

        Map<ResourceID, ? extends Set<? extends SlotInfo>> slotsByTaskExecutor =
                rawFreeSlots.stream()
                        .collect(
                                Collectors.groupingBy(
                                        slotInfo ->
                                                slotInfo.getTaskManagerLocation().getResourceID(),
                                        Collectors.mapping(identity(), Collectors.toSet())));

        List<ResourceID> orderedTaskExecutors =
                getSortedTaskExecutors(rawFreeSlots, scores, slotsByTaskExecutor);

        for (ResourceID resourceID : orderedTaskExecutors) {
            Set<? extends SlotInfo> slotInfos = slotsByTaskExecutor.get(resourceID);
            if (redundantSlots >= slotInfos.size()) {
                redundantSlots -= slotInfos.size();
                rawFreeSlots.removeAll(slotInfos);
            } else {
                break;
            }
        }
        return rawFreeSlots;
    }

    /**
     * Get task executors in the special ascending order, which is sorted by the number of slots and
     * the summary allocation scores.
     */
    private static List<ResourceID> getSortedTaskExecutors(
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
            final ResourceID resourceID = allocationIdToResourceId.get(allocScore.allocationId);
            if (Objects.nonNull(resourceID)) {
                resourceScores.compute(
                        resourceID,
                        (rid, oldVal) ->
                                Objects.isNull(oldVal)
                                        ? allocScore.score
                                        : oldVal + allocScore.score);
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
