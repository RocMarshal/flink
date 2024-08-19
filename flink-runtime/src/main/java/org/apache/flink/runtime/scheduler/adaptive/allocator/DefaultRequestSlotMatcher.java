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
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadingUtilization;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.adaptive.allocator.StateLocalitySlotAssigner.AllocationScore;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The default implementation of {@link
 * org.apache.flink.runtime.scheduler.adaptive.allocator.RequestSlotMatcher}.
 */
public class DefaultRequestSlotMatcher implements RequestSlotMatcher {

    @Override
    public Collection<SlotAssignment> matchRequestsWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<? extends SlotInfo> freeSlots,
            TaskExecutorsLoadingUtilization taskExecutorsLoadingUtilization,
            @Nonnull Queue<AllocationScore> scores) {

        if (scores.isEmpty()) {
            return matchWithoutScore(requestGroups, freeSlots);
        }

        final Map<AllocationID, SlotInfo> slotsById =
                freeSlots.stream().collect(toMap(SlotInfo::getAllocationId, identity()));

        final Map<String, ExecutionSlotSharingGroup> groupsById =
                requestGroups.stream().collect(toMap(ExecutionSlotSharingGroup::getId, identity()));

        AllocationScore score;
        final Collection<SlotAssignment> assignments = new ArrayList<>();
        while ((score = scores.poll()) != null) {
            if (slotsById.containsKey(score.getAllocationId())
                    && groupsById.containsKey(score.getGroupId())) {
                assignments.add(
                        new SlotAssignment(
                                slotsById.remove(score.getAllocationId()),
                                groupsById.remove(score.getGroupId())));
            }
        }

        // Distribute the remaining slots with no score
        Collection<SlotAssignment> remainingAssignments =
                matchWithoutScore(groupsById.values(), slotsById.values());

        assignments.addAll(remainingAssignments);
        return assignments;
    }

    private static Collection<SlotAssignment> matchWithoutScore(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<? extends SlotInfo> slots) {
        Iterator<? extends SlotInfo> slotIterator = slots.iterator();
        final Collection<SlotAssignment> assignments = new ArrayList<>();
        for (ExecutionSlotSharingGroup group : requestGroups) {
            checkState(
                    slotIterator.hasNext(),
                    "No slots available for group %s (%s more in total). This is likely a bug.",
                    group,
                    slots.size());
            assignments.add(new SlotAssignment(slotIterator.next(), group));
            slotIterator.remove();
        }
        return assignments;
    }
}
