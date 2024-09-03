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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobAllocationsInformation.VertexAllocationInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.DefaultSlotAssigner.createExecutionSlotSharingGroups;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link SlotAssigner} that assigns slots based on the number of local key groups. */
@Internal
public class StateLocalitySlotAssigner implements SlotAssigner {

    public static class AllocationScore implements Comparable<AllocationScore> {

        private final String groupId;
        private final AllocationID allocationId;

        public AllocationScore(String groupId, AllocationID allocationId, long score) {
            this.groupId = groupId;
            this.allocationId = allocationId;
            this.score = score;
        }

        private final long score;

        public String getGroupId() {
            return groupId;
        }

        public AllocationID getAllocationId() {
            return allocationId;
        }

        public long getScore() {
            return score;
        }

        @Override
        public int compareTo(StateLocalitySlotAssigner.AllocationScore other) {
            int result = Long.compare(score, other.score);
            if (result != 0) {
                return result;
            }
            result = other.allocationId.compareTo(allocationId);
            if (result != 0) {
                return result;
            }
            return other.groupId.compareTo(groupId);
        }
    }

    @Override
    public Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism,
            JobAllocationsInformation previousAllocations) {
        checkState(
                freeSlots.size() >= jobInformation.getSlotSharingGroups().size(),
                "Not enough slots to allocate all the slot sharing groups (have: %s, need: %s)",
                freeSlots.size(),
                jobInformation.getSlotSharingGroups().size());

        final List<ExecutionSlotSharingGroup> allGroups = new ArrayList<>();
        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            allGroups.addAll(createExecutionSlotSharingGroups(vertexParallelism, slotSharingGroup));
        }
        final Map<JobVertexID, Integer> parallelism = getParallelism(allGroups);
        final PriorityQueue<AllocationScore> scores =
                calculateScores(jobInformation, previousAllocations, allGroups, parallelism);

        final Map<AllocationID, SlotInfo> slotsById =
                filterTargetSlots(freeSlots, allGroups, scores).stream()
                        .collect(toMap(SlotInfo::getAllocationId, identity()));

        final Map<String, ExecutionSlotSharingGroup> groupsById =
                allGroups.stream().collect(toMap(ExecutionSlotSharingGroup::getId, identity()));

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
        Iterator<? extends SlotInfo> remainingSlots = slotsById.values().iterator();
        for (ExecutionSlotSharingGroup group : groupsById.values()) {
            checkState(
                    remainingSlots.hasNext(),
                    "No slots available for group %s (%s more in total). This is likely a bug.",
                    group,
                    groupsById.size());
            assignments.add(new SlotAssignment(remainingSlots.next(), group));
            remainingSlots.remove();
        }

        return assignments;
    }

    /** Evict some redundant slots for the available slots in minimum task executors. */
    @VisibleForTesting
    public static Collection<? extends SlotInfo> filterTargetSlots(
            Collection<? extends SlotInfo> freeSlots,
            List<ExecutionSlotSharingGroup> allGroups,
            PriorityQueue<AllocationScore> scores) {

        int redundantSlots = freeSlots.size() - allGroups.size();
        if (redundantSlots <= 0) {
            return freeSlots;
        }

        final Set<SlotInfo> result = new HashSet<>(freeSlots);
        Map<ResourceID, ? extends Set<? extends SlotInfo>> slotsByTaskExecutor =
                freeSlots.stream()
                        .collect(
                                Collectors.groupingBy(
                                        slotInfo ->
                                                slotInfo.getTaskManagerLocation().getResourceID(),
                                        Collectors.mapping(identity(), Collectors.toSet())));

        List<ResourceID> orderedTaskExecutors =
                getSortedTaskExecutors(freeSlots, scores, slotsByTaskExecutor);

        for (ResourceID resourceID : orderedTaskExecutors) {
            Set<? extends SlotInfo> slotInfos = slotsByTaskExecutor.get(resourceID);
            if (redundantSlots >= slotInfos.size()) {
                redundantSlots -= slotInfos.size();
                result.removeAll(slotInfos);
            } else {
                break;
            }
        }
        return result;
    }

    /**
     * Get task executors in the special ascending order, which is sorted by the number of slots and
     * the summary allocation scores.
     */
    private static List<ResourceID> getSortedTaskExecutors(
            Collection<? extends SlotInfo> freeSlots,
            PriorityQueue<AllocationScore> scores,
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

    @Nonnull
    private PriorityQueue<AllocationScore> calculateScores(
            JobInformation jobInformation,
            JobAllocationsInformation previousAllocations,
            List<ExecutionSlotSharingGroup> allGroups,
            Map<JobVertexID, Integer> parallelism) {
        // PQ orders the pairs (allocationID, groupID) by score, decreasing
        // the score is computed as the potential amount of state that would reside locally
        final PriorityQueue<AllocationScore> scores =
                new PriorityQueue<>(Comparator.reverseOrder());
        for (ExecutionSlotSharingGroup group : allGroups) {
            scores.addAll(calculateScore(group, parallelism, jobInformation, previousAllocations));
        }
        return scores;
    }

    private static Map<JobVertexID, Integer> getParallelism(
            List<ExecutionSlotSharingGroup> groups) {
        final Map<JobVertexID, Integer> parallelism = new HashMap<>();
        for (ExecutionSlotSharingGroup group : groups) {
            for (ExecutionVertexID evi : group.getContainedExecutionVertices()) {
                parallelism.merge(evi.getJobVertexId(), 1, Integer::sum);
            }
        }
        return parallelism;
    }

    public Collection<AllocationScore> calculateScore(
            ExecutionSlotSharingGroup group,
            Map<JobVertexID, Integer> parallelism,
            JobInformation jobInformation,
            JobAllocationsInformation previousAllocations) {
        final Map<AllocationID, Long> score = new HashMap<>();
        for (ExecutionVertexID evi : group.getContainedExecutionVertices()) {
            final KeyGroupRange kgr =
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                            jobInformation
                                    .getVertexInformation(evi.getJobVertexId())
                                    .getMaxParallelism(),
                            parallelism.get(evi.getJobVertexId()),
                            evi.getSubtaskIndex());
            previousAllocations
                    .getAllocations(evi.getJobVertexId())
                    .forEach(
                            allocation -> {
                                long value = estimateSize(kgr, allocation);
                                if (value > 0) {
                                    score.merge(allocation.getAllocationID(), value, Long::sum);
                                }
                            });
        }

        return score.entrySet().stream()
                .map(e -> new AllocationScore(group.getId(), e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    private static long estimateSize(
            KeyGroupRange newRange, VertexAllocationInformation allocation) {
        KeyGroupRange oldRange = allocation.getKeyGroupRange();
        if (allocation.stateSizeInBytes * oldRange.getNumberOfKeyGroups() == 0) {
            return 0L;
        }
        // round up to 1
        long keyGroupSize =
                allocation.stateSizeInBytes
                        / Math.min(allocation.stateSizeInBytes, oldRange.getNumberOfKeyGroups());
        int numberOfKeyGroups = oldRange.getIntersection(newRange).getNumberOfKeyGroups();
        return numberOfKeyGroups * keyGroupSize;
    }
}
