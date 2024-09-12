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
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.adaptive.allocator.AllocationScore.compares;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.AllocationScore.toKey;
import static org.apache.flink.runtime.scheduler.loading.WeightLoadable.sortByLoadingDescend;

/** The balanced request slot matching matcher implementation. */
public class TaskBalancedRequestSlotMatcher implements RequestSlotMatcher {

    @Override
    public Collection<SlotAssignment> matchRequestsWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<? extends SlotInfo> freeSlots,
            TaskExecutorsLoadingUtilization taskExecutorsLoadingUtilization,
            @Nonnull Queue<AllocationScore> scores) {
        final List<SlotAssignment> slotAssignments = new ArrayList<>(requestGroups.size());
        final Map<AllocationScore.ScoreKey, AllocationScore> scoreMap =
                RequestSlotMatcher.getScoreMap(scores);
        final Map<ResourceID, LoadingWeight> taskExecutorLoadings =
                taskExecutorsLoadingUtilization.getTaskExecutorsLoadingWeight();
        final Map<ResourceID, Set<SlotInfo>> slotsPerTaskExecutor =
                RequestSlotMatcher.getSlotsPerTaskExecutor(freeSlots);
        final Map<LoadingWeight, Set<SlotInfo>> loadingSlotsMap =
                getLoadingSlotsMap(freeSlots, taskExecutorLoadings);

        SlotTaskExecutorWeight<LoadingWeight> best;
        for (ExecutionSlotSharingGroup requestGroup : sortByLoadingDescend(requestGroups)) {
            best = getTheBestSlotTaskExecutorLoading(loadingSlotsMap, requestGroup, scoreMap);
            slotAssignments.add(new SlotAssignment(best.slotInfo, requestGroup));

            // Update the references
            final LoadingWeight newLoading =
                    best.taskExecutorWeight.merge(requestGroup.getLoading());
            taskExecutorLoadings.put(best.getResourceID(), newLoading);
            scoreMap.remove(toKey(requestGroup, best.slotInfo));
            updateSlotsPerTaskExecutor(slotsPerTaskExecutor, best);
            Set<SlotInfo> slotInfos = slotsPerTaskExecutor.get(best.getResourceID());
            updateLoadingSlotsMap(loadingSlotsMap, best, slotInfos, newLoading);
        }
        return slotAssignments;
    }

    private static void updateLoadingSlotsMap(
            Map<LoadingWeight, Set<SlotInfo>> loadingSlotsMap,
            SlotTaskExecutorWeight<LoadingWeight> best,
            Set<SlotInfo> slotsToAdjust,
            LoadingWeight newLoading) {
        Set<SlotInfo> slotInfos = loadingSlotsMap.get(best.taskExecutorWeight);
        slotInfos.remove(best.slotInfo);
        if (Objects.nonNull(slotsToAdjust)) {
            slotInfos.removeAll(slotsToAdjust);
        }
        if (slotInfos.isEmpty()) {
            loadingSlotsMap.remove(best.taskExecutorWeight);
        }
        if (Objects.nonNull(slotsToAdjust)) {
            Set<SlotInfo> slotsOfNewKey =
                    loadingSlotsMap.computeIfAbsent(
                            newLoading, slotsUtilization -> new HashSet<>());
            slotsOfNewKey.addAll(slotsToAdjust);
        }
    }

    private static void updateSlotsPerTaskExecutor(
            Map<ResourceID, Set<SlotInfo>> slotsPerTaskExecutor,
            SlotTaskExecutorWeight<LoadingWeight> best) {
        Set<SlotInfo> slotInfos = slotsPerTaskExecutor.get(best.getResourceID());
        slotInfos.remove(best.slotInfo);
        if (slotInfos.isEmpty()) {
            slotsPerTaskExecutor.remove(best.getResourceID());
        }
    }

    private static Map<LoadingWeight, Set<SlotInfo>> getLoadingSlotsMap(
            Collection<? extends SlotInfo> slots,
            Map<ResourceID, LoadingWeight> taskExecutorLoadings) {
        return slots.stream()
                .collect(
                        Collectors.groupingBy(
                                slotInfo ->
                                        taskExecutorLoadings.get(
                                                slotInfo.getTaskManagerLocation().getResourceID()),
                                TreeMap::new,
                                Collectors.toSet()));
    }

    private static SlotTaskExecutorWeight<LoadingWeight> getTheBestSlotTaskExecutorLoading(
            Map<LoadingWeight, Set<SlotInfo>> slotsByLoading,
            ExecutionSlotSharingGroup requestGroup,
            Map<AllocationScore.ScoreKey, AllocationScore> scoreMap) {
        final LoadingWeight loadingWeight =
                slotsByLoading.keySet().stream()
                        .min(LoadingWeight::compareTo)
                        .orElseThrow(NO_SUITABLE_SLOTS_EXCEPTION_GETTER);
        final SlotInfo targetSlot =
                slotsByLoading.get(loadingWeight).stream()
                        .max(
                                (left, right) ->
                                        compares(
                                                scoreMap.get(toKey(requestGroup, left)),
                                                scoreMap.get(toKey(requestGroup, right))))
                        .orElseThrow(NO_SUITABLE_SLOTS_EXCEPTION_GETTER);
        return new SlotTaskExecutorWeight<>(loadingWeight, targetSlot);
    }
}
