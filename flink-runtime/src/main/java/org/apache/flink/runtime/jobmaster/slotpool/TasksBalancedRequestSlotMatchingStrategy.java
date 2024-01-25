/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.scheduler.loading.WeightLoadable;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The tasks balanced based implementation of {@link RequestSlotMatchingStrategy} that matches the
 * pending requests for tasks balance at task-manager level.
 */
public enum TasksBalancedRequestSlotMatchingStrategy implements RequestSlotMatchingStrategy {
    INSTANCE;

    /** The comparator to compare loading. */
    static final class DynamicSlotLoadingComparator implements Comparator<PhysicalSlot> {

        private final Map<TaskManagerLocation, LoadingWeight> taskExecutorsLoading;

        DynamicSlotLoadingComparator(Map<TaskManagerLocation, LoadingWeight> taskExecutorsLoading) {
            this.taskExecutorsLoading = Preconditions.checkNotNull(taskExecutorsLoading);
        }

        @Override
        public int compare(PhysicalSlot left, PhysicalSlot right) {
            final LoadingWeight leftLoad =
                    taskExecutorsLoading.getOrDefault(
                            left.getTaskManagerLocation(), LoadingWeight.EMPTY);
            final LoadingWeight rightLoad =
                    taskExecutorsLoading.getOrDefault(
                            right.getTaskManagerLocation(), LoadingWeight.EMPTY);
            return leftLoad.compareTo(rightLoad);
        }
    }

    @Override
    public Collection<RequestSlotMatch> matchRequestsAndSlots(
            Collection<? extends PhysicalSlot> slots,
            Collection<PendingRequest> pendingRequests,
            Map<TaskManagerLocation, LoadingWeight> taskExecutorsLoad) {
        if (pendingRequests.isEmpty()) {
            return Collections.emptyList();
        }

        final Collection<RequestSlotMatch> resultingMatches = new ArrayList<>();
        final List<PendingRequest> sortedRequests =
                WeightLoadable.sortByLoadingDescend(pendingRequests);
        final Map<ResourceProfile, PriorityQueue<PhysicalSlot>> profileToSlotMap =
                getSlotCandidatesByResourceProfile(slots, taskExecutorsLoad);
        final Map<TaskManagerLocation, Set<PhysicalSlot>> slotsPerTaskExecutor =
                slots.stream()
                        .collect(
                                Collectors.groupingBy(
                                        PhysicalSlot::getTaskManagerLocation, Collectors.toSet()));
        for (PendingRequest request : sortedRequests) {
            Optional<PhysicalSlot> bestSlot =
                    tryMatchPhysicalSlot(request, profileToSlotMap, taskExecutorsLoad);
            if (bestSlot.isPresent()) {
                updateReferenceAfterMatching(
                        profileToSlotMap,
                        taskExecutorsLoad,
                        slotsPerTaskExecutor,
                        bestSlot.get(),
                        request.getLoading());
                resultingMatches.add(RequestSlotMatch.createFor(request, bestSlot.get()));
            }
        }
        return resultingMatches;
    }

    private Map<ResourceProfile, PriorityQueue<PhysicalSlot>> getSlotCandidatesByResourceProfile(
            @Nonnull Collection<? extends PhysicalSlot> slots,
            Map<TaskManagerLocation, LoadingWeight> taskExecutorsLoad) {
        final Map<ResourceProfile, PriorityQueue<PhysicalSlot>> result = new HashMap<>();
        final DynamicSlotLoadingComparator comparator =
                new DynamicSlotLoadingComparator(taskExecutorsLoad);
        for (PhysicalSlot slot : slots) {
            result.compute(
                    slot.getResourceProfile(),
                    (resourceProfile, oldSlots) -> {
                        PriorityQueue<PhysicalSlot> values =
                                Objects.isNull(oldSlots)
                                        ? new PriorityQueue<>(comparator)
                                        : oldSlots;
                        values.add(slot);
                        return values;
                    });
        }
        return result;
    }

    private Optional<PhysicalSlot> tryMatchPhysicalSlot(
            PendingRequest request,
            Map<ResourceProfile, PriorityQueue<PhysicalSlot>> profileToSlotMap,
            Map<TaskManagerLocation, LoadingWeight> taskExecutorsLoad) {
        final ResourceProfile requestProfile = request.getResourceProfile();

        final Set<ResourceProfile> candidateProfiles =
                profileToSlotMap.keySet().stream()
                        .filter(slotProfile -> slotProfile.isMatching(requestProfile))
                        .collect(Collectors.toSet());

        return candidateProfiles.stream()
                .map(
                        candidateProfile -> {
                            PriorityQueue<PhysicalSlot> slots =
                                    profileToSlotMap.get(candidateProfile);
                            return CollectionUtil.isNullOrEmpty(slots) ? null : slots.peek();
                        })
                .filter(Objects::nonNull)
                .min(new DynamicSlotLoadingComparator(taskExecutorsLoad));
    }

    private void updateReferenceAfterMatching(
            Map<ResourceProfile, PriorityQueue<PhysicalSlot>> profileToSlotMap,
            Map<TaskManagerLocation, LoadingWeight> taskExecutorsLoad,
            Map<TaskManagerLocation, Set<PhysicalSlot>> slotsPerTaskExecutor,
            PhysicalSlot targetSlot,
            LoadingWeight loading) {
        final ResourceProfile slotProfile = targetSlot.getResourceProfile();

        // update the loading for the target task executor.
        taskExecutorsLoad.compute(
                targetSlot.getTaskManagerLocation(),
                (taskManagerLocation, oldLoading) ->
                        oldLoading == null ? loading : oldLoading.merge(loading));
        // update the sorted set for slots that is located on the same task executor as targetSlot.
        final TaskManagerLocation tmLocation = targetSlot.getTaskManagerLocation();
        // Use Map#remove to avoid the ConcurrentModifyException.
        final Set<PhysicalSlot> slotToReSort = slotsPerTaskExecutor.remove(tmLocation);
        for (PhysicalSlot slot : slotToReSort) {
            PriorityQueue<PhysicalSlot> slotsOfProfile =
                    profileToSlotMap.get(slot.getResourceProfile());
            // Re-add for the latest order.
            slotsOfProfile.remove(slot);
            if (!slot.equals(targetSlot)) {
                slotsOfProfile.add(slot);
            }
        }
        slotToReSort.remove(targetSlot);
        slotsPerTaskExecutor.put(tmLocation, slotToReSort);
    }

    @Override
    public String toString() {
        return TasksBalancedRequestSlotMatchingStrategy.class.getSimpleName();
    }
}
