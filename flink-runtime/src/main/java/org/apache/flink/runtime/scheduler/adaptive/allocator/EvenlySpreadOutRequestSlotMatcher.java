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
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadingUtilization.SlotsUtilization;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.StateLocalitySlotAssigner.ScoreKey;

/** The evenly spread out request slot matching matcher implementation. */
public class EvenlySpreadOutRequestSlotMatcher implements RequestSlotMatcher {

    @Override
    public Collection<SlotAssignment> matchRequestsWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<? extends SlotInfo> freeSlots,
            TaskExecutorsLoadingUtilization taskExecutorsLoadingUtilization,
            @Nonnull Queue<AllocationScore> scores) {
        final List<SlotAssignment> slotAssignments = new ArrayList<>(requestGroups.size());
        Map<ResourceID, SlotsUtilization> slotsUtilizations =
                taskExecutorsLoadingUtilization.getTaskExecutorsSlotsUtilization();
        SlotInfoUtilization bestSlotWithUtilization;

        if (scores.isEmpty()) {
            for (ExecutionSlotSharingGroup requestGroup : requestGroups) {
                bestSlotWithUtilization = findMinSlotInfoUtilization(freeSlots, slotsUtilizations);
                SlotInfo slotInfo = Preconditions.checkNotNull(bestSlotWithUtilization.slotInfo);
                SlotsUtilization slotsUtilization = bestSlotWithUtilization.slotsUtilization;

                slotAssignments.add(new SlotAssignment(slotInfo, requestGroup));
                slotsUtilizations.put(
                        slotInfo.getTaskManagerLocation().getResourceID(),
                        slotsUtilization.incReserved(1));
                freeSlots.remove(slotInfo);
            }
        } else {
            Map<ScoreKey, Long> scoreMap = getScoreMap(scores);
            for (ExecutionSlotSharingGroup requestGroup : requestGroups) {
                bestSlotWithUtilization =
                        findMinSlotsUtilizationWithMaxScore(
                                freeSlots, requestGroup, slotsUtilizations, scoreMap);
                SlotInfo slotInfo = Preconditions.checkNotNull(bestSlotWithUtilization.slotInfo);
                SlotsUtilization slotsUtilization = bestSlotWithUtilization.slotsUtilization;

                slotAssignments.add(new SlotAssignment(slotInfo, requestGroup));
                slotsUtilizations.put(
                        slotInfo.getTaskManagerLocation().getResourceID(),
                        slotsUtilization.incReserved(1));
                freeSlots.remove(slotInfo);
                scoreMap.remove(new ScoreKey(requestGroup.getId(), slotInfo.getAllocationId()));
            }
        }
        return slotAssignments;
    }

    private SlotInfoUtilization findMinSlotsUtilizationWithMaxScore(
            Collection<? extends SlotInfo> freeSlots,
            ExecutionSlotSharingGroup requestGroup,
            Map<ResourceID, SlotsUtilization> slotsUtilizations,
            Map<ScoreKey, Long> scoreMap) {

        double minUtilization = findMinSlotsUtilization(slotsUtilizations);
        return freeSlots.stream()
                .map(slotInfo -> toSlotInfoUtilization(slotsUtilizations, slotInfo))
                .filter(
                        slotInfoUtilization ->
                                slotInfoUtilization.hasUtilizationEquals(minUtilization))
                .max(Comparator.comparingLong(value -> getScore(requestGroup, value, scoreMap)))
                .orElseThrow(() -> new FlinkRuntimeException("No enough slots!"));
    }

    private static double findMinSlotsUtilization(
            Map<ResourceID, SlotsUtilization> slotsUtilizations) {
        return slotsUtilizations.values().stream()
                .min(Comparator.comparingDouble(SlotsUtilization::getUtilization))
                .orElseThrow(() -> new FlinkRuntimeException("No suitable slots!"))
                .getUtilization();
    }

    private SlotInfoUtilization findMinSlotInfoUtilization(
            Collection<? extends SlotInfo> freeSlots,
            Map<ResourceID, SlotsUtilization> slotsUtilizations) {
        return freeSlots.stream()
                .map(slotInfo -> toSlotInfoUtilization(slotsUtilizations, slotInfo))
                .min(Comparator.comparingDouble(value -> value.slotsUtilization.getUtilization()))
                .orElseThrow(() -> new FlinkRuntimeException("No enough slots!"));
    }

    private static Long getScore(
            ExecutionSlotSharingGroup requestGroup,
            SlotInfoUtilization value,
            Map<ScoreKey, Long> scoreMap) {
        return scoreMap.getOrDefault(
                new ScoreKey(requestGroup.getId(), value.slotInfo.getAllocationId()),
                Long.MIN_VALUE);
    }

    private static Map<ScoreKey, Long> getScoreMap(Queue<AllocationScore> scores) {
        return scores.stream()
                .collect(Collectors.toMap(ScoreKey::new, allocationScore -> allocationScore.score));
    }

    private SlotInfoUtilization toSlotInfoUtilization(
            Map<ResourceID, SlotsUtilization> slotsUtilizations, SlotInfo slotInfo) {
        final ResourceID resourceID = slotInfo.getTaskManagerLocation().getResourceID();
        return new SlotInfoUtilization(slotsUtilizations.get(resourceID), slotInfo);
    }

    private static class SlotInfoUtilization {
        private final SlotsUtilization slotsUtilization;
        private final SlotInfo slotInfo;

        SlotInfoUtilization(SlotsUtilization slotsUtilization, SlotInfo slotInfo) {
            this.slotsUtilization = slotsUtilization;
            this.slotInfo = slotInfo;
        }

        boolean hasUtilizationEquals(double utilization) {
            return slotsUtilization.getUtilization() == utilization;
        }
    }
}
