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

import static org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadingUtilization.SlotsUtilization;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.AllocationScore.compares;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.AllocationScore.toKey;

/** The evenly spread out request slot matching matcher implementation. */
public class EvenlySpreadOutRequestSlotMatcher implements RequestSlotMatcher {

    @Override
    public Collection<SlotAssignment> matchRequestsWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<? extends SlotInfo> freeSlots,
            TaskExecutorsLoadingUtilization taskExecutorsLoadingUtilization,
            @Nonnull Queue<AllocationScore> scores) {

        final List<SlotAssignment> slotAssignments = new ArrayList<>(requestGroups.size());
        final Map<AllocationScore.ScoreKey, AllocationScore> scoreMap =
                RequestSlotMatcher.getScoreMap(scores);
        final Map<ResourceID, SlotsUtilization> taskExecutorSlotsUtilizations =
                taskExecutorsLoadingUtilization.getTaskExecutorsSlotsUtilization();
        final Map<ResourceID, Set<SlotInfo>> slotsPerTaskExecutor =
                RequestSlotMatcher.getSlotsPerTaskExecutor(freeSlots);
        final Map<SlotsUtilization, Set<SlotInfo>> utilizationSlotsMap =
                getUtilizationSlotsMap(freeSlots, taskExecutorSlotsUtilizations);

        SlotTaskExecutorWeight<SlotsUtilization> best;
        for (ExecutionSlotSharingGroup requestGroup : requestGroups) {
            best = getTheBestSlotUtilization(utilizationSlotsMap, requestGroup, scoreMap);
            slotAssignments.add(new SlotAssignment(best.slotInfo, requestGroup));

            // Update the references
            final SlotsUtilization newSlotsUtilization = best.taskExecutorWeight.incReserved(1);
            taskExecutorSlotsUtilizations.put(best.getResourceID(), newSlotsUtilization);
            scoreMap.remove(toKey(requestGroup, best.slotInfo));
            updateSlotsPerTaskExecutor(slotsPerTaskExecutor, best);
            Set<SlotInfo> slotInfos = slotsPerTaskExecutor.get(best.getResourceID());
            updateUtilizationSlotsMap(utilizationSlotsMap, best, slotInfos, newSlotsUtilization);
        }
        return slotAssignments;
    }

    private static void updateUtilizationSlotsMap(
            Map<SlotsUtilization, Set<SlotInfo>> utilizationSlotsMap,
            SlotTaskExecutorWeight<SlotsUtilization> best,
            Set<SlotInfo> slotsToAdjust,
            SlotsUtilization newSlotsUtilization) {
        Set<SlotInfo> slotInfos = utilizationSlotsMap.get(best.taskExecutorWeight);
        slotInfos.remove(best.slotInfo);
        if (Objects.nonNull(slotsToAdjust)) {
            slotInfos.removeAll(slotsToAdjust);
        }
        if (slotInfos.isEmpty()) {
            utilizationSlotsMap.remove(best.taskExecutorWeight);
        }
        if (Objects.nonNull(slotsToAdjust)) {
            Set<SlotInfo> slotsOfNewKey =
                    utilizationSlotsMap.computeIfAbsent(
                            newSlotsUtilization, slotsUtilization -> new HashSet<>());
            slotsOfNewKey.addAll(slotsToAdjust);
        }
    }

    private static void updateSlotsPerTaskExecutor(
            Map<ResourceID, Set<SlotInfo>> slotsPerTaskExecutor,
            SlotTaskExecutorWeight<SlotsUtilization> best) {
        Set<SlotInfo> slotInfos = slotsPerTaskExecutor.get(best.getResourceID());
        slotInfos.remove(best.slotInfo);
        if (slotInfos.isEmpty()) {
            slotsPerTaskExecutor.remove(best.getResourceID());
        }
    }

    private static Map<SlotsUtilization, Set<SlotInfo>> getUtilizationSlotsMap(
            Collection<? extends SlotInfo> slots,
            Map<ResourceID, SlotsUtilization> slotsUtilizations) {
        return slots.stream()
                .collect(
                        Collectors.groupingBy(
                                slotInfo ->
                                        slotsUtilizations.get(
                                                slotInfo.getTaskManagerLocation().getResourceID()),
                                TreeMap::new,
                                Collectors.toSet()));
    }

    private static SlotTaskExecutorWeight<SlotsUtilization> getTheBestSlotUtilization(
            Map<SlotsUtilization, Set<SlotInfo>> slotsByUtilization,
            ExecutionSlotSharingGroup requestGroup,
            Map<AllocationScore.ScoreKey, AllocationScore> scoreMap) {
        final SlotsUtilization slotsUtilization =
                slotsByUtilization.keySet().stream()
                        .filter(su -> !su.isFullUtilization())
                        .min(SlotsUtilization::compareTo)
                        .orElseThrow(NO_SUITABLE_SLOTS_EXCEPTION_GETTER);
        final SlotInfo targetSlot =
                slotsByUtilization.get(slotsUtilization).stream()
                        .max(
                                (left, right) ->
                                        compares(
                                                scoreMap.get(toKey(requestGroup, left)),
                                                scoreMap.get(toKey(requestGroup, right))))
                        .orElseThrow(NO_SUITABLE_SLOTS_EXCEPTION_GETTER);
        return new SlotTaskExecutorWeight<>(slotsUtilization, targetSlot);
    }
}
