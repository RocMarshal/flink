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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.loading.DefaultLoadingWeight;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.scheduler.loading.WeightLoadable;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.runtime.state.heap.AbstractHeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueue;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The tasks balanced based implementation of {@link RequestSlotMatchingStrategy} that matches the
 * pending requests for tasks balance at task-manager level.
 */
public enum TasksBalancedRequestSlotMatchingStrategy implements RequestSlotMatchingStrategy {
    INSTANCE;

    public static final Logger LOG =
            LoggerFactory.getLogger(TasksBalancedRequestSlotMatchingStrategy.class);

    /** The {@link SlotInfoElement} comparator to compare loading. */
    public static final class SlotInfoElementComparator<T extends SlotInfo>
            implements Comparator<SlotInfoElement<T>> {

        private final Map<ResourceID, LoadingWeight> taskExecutorsLoading;

        public SlotInfoElementComparator(Map<ResourceID, LoadingWeight> taskExecutorsLoading) {
            this.taskExecutorsLoading = Preconditions.checkNotNull(taskExecutorsLoading);
        }

        @Override
        public int compare(SlotInfoElement<T> left, SlotInfoElement<T> right) {
            final LoadingWeight leftLoad =
                    taskExecutorsLoading.getOrDefault(
                            left.slotInfo.getTaskManagerLocation().getResourceID(),
                            DefaultLoadingWeight.EMPTY);
            final LoadingWeight rightLoad =
                    taskExecutorsLoading.getOrDefault(
                            right.slotInfo.getTaskManagerLocation().getResourceID(),
                            DefaultLoadingWeight.EMPTY);
            return leftLoad.compareTo(rightLoad);
        }
    }

    /**
     * The {@link org.apache.flink.runtime.jobmaster.SlotInfo} element wrapper for {@link
     * HeapPriorityQueue}.
     */
    public static final class SlotInfoElement<T extends SlotInfo>
            extends AbstractHeapPriorityQueueElement {

        final T slotInfo;

        public SlotInfoElement(T slotInfo) {
            this.slotInfo = slotInfo;
        }

        public T getSlotInfo() {
            return slotInfo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SlotInfoElement<?> that = (SlotInfoElement<?>) o;
            return Objects.equals(slotInfo, that.slotInfo);
        }

        @Override
        public int hashCode() {
            return slotInfo.hashCode();
        }
    }

    /** The {@link SlotInfoElement} comparator. */
    public static final class SlotElementPriorityComparator<T extends SlotInfo>
            implements PriorityComparator<SlotInfoElement<T>> {

        private final SlotInfoElementComparator<T> slotInfoElementComparator;

        public SlotElementPriorityComparator(
                Map<ResourceID, LoadingWeight> taskExecutorsLoading) {
            this.slotInfoElementComparator = new SlotInfoElementComparator<>(taskExecutorsLoading);
        }

        @Override
        public int comparePriority(SlotInfoElement<T> left, SlotInfoElement<T> right) {
            return slotInfoElementComparator.compare(left, right);
        }
    }

    @Override
    public Collection<RequestSlotMatch> matchRequestsAndSlots(
            Collection<? extends PhysicalSlot> slots,
            Collection<PendingRequest> pendingRequests,
            Map<ResourceID, LoadingWeight> taskExecutorsLoad) {
        if (pendingRequests.isEmpty()) {
            return Collections.emptyList();
        }

        final Collection<RequestSlotMatch> resultingMatches = new ArrayList<>();
        final List<PendingRequest> sortedRequests = sortByLoadingDescend(pendingRequests);
        LOG.debug(
                "Available slots: {}, sortedRequests: {}, taskExecutorsLoad: {}",
                slots,
                sortedRequests,
                taskExecutorsLoad);
        Collection<SlotInfoElement<PhysicalSlot>> slotInfoElements =
                slots.stream()
                        .map(
                                (Function<PhysicalSlot, SlotInfoElement<PhysicalSlot>>)
                                        SlotInfoElement::new)
                        .collect(Collectors.toList());
        final Map<ResourceProfile, HeapPriorityQueue<SlotInfoElement<PhysicalSlot>>> profileSlots =
                getSlotCandidatesByProfile(slotInfoElements, taskExecutorsLoad);
        final Map<ResourceID, Set<SlotInfoElement<PhysicalSlot>>> taskExecutorSlots =
                groupSlotsByTaskExecutor(slotInfoElements);
        for (PendingRequest request : sortedRequests) {
            Optional<SlotInfoElement<PhysicalSlot>> bestSlotEle =
                    tryMatchPhysicalSlot(request, profileSlots, taskExecutorsLoad);
            if (bestSlotEle.isPresent()) {
                SlotInfoElement<PhysicalSlot> slotInfoElement = bestSlotEle.get();
                updateReferenceAfterMatching(
                        profileSlots,
                        taskExecutorsLoad,
                        taskExecutorSlots,
                        slotInfoElement,
                        request.getLoading());
                resultingMatches.add(
                        RequestSlotMatch.createFor(request, slotInfoElement.getSlotInfo()));
            }
        }
        return resultingMatches;
    }

    private Map<ResourceID, Set<SlotInfoElement<PhysicalSlot>>> groupSlotsByTaskExecutor(
            Collection<SlotInfoElement<PhysicalSlot>> slotInfoElements) {
        return slotInfoElements.stream()
                .collect(
                        Collectors.groupingBy(
                                physicalSlot ->
                                        physicalSlot
                                                .slotInfo
                                                .getTaskManagerLocation()
                                                .getResourceID(),
                                Collectors.toSet()));
    }

    private <T extends WeightLoadable> List<T> sortByLoadingDescend(Collection<T> pendingRequests) {
        return pendingRequests.stream()
                .sorted(
                        (leftReq, rightReq) ->
                                rightReq.getLoading().compareTo(leftReq.getLoading()))
                .collect(Collectors.toList());
    }

    private Map<ResourceProfile, HeapPriorityQueue<SlotInfoElement<PhysicalSlot>>>
            getSlotCandidatesByProfile(
                    Collection<SlotInfoElement<PhysicalSlot>> slotInfoElements,
                    Map<ResourceID, LoadingWeight> taskExecutorsLoad) {
        final Map<ResourceProfile, HeapPriorityQueue<SlotInfoElement<PhysicalSlot>>> result =
                new HashMap<>();
        final SlotElementPriorityComparator<PhysicalSlot>
                slotElementPriorityComparator =
                        new SlotElementPriorityComparator<>(taskExecutorsLoad);
        for (SlotInfoElement<PhysicalSlot> slotEle : slotInfoElements) {
            result.compute(
                    slotEle.slotInfo.getResourceProfile(),
                    (resourceProfile, oldSlots) -> {
                        HeapPriorityQueue<SlotInfoElement<PhysicalSlot>> values =
                                Objects.isNull(oldSlots)
                                        ? new HeapPriorityQueue<>(
                                        slotElementPriorityComparator, 8)
                                        : oldSlots;
                        values.add(slotEle);
                        return values;
                    });
        }
        return result;
    }

    private Optional<SlotInfoElement<PhysicalSlot>> tryMatchPhysicalSlot(
            PendingRequest request,
            Map<ResourceProfile, HeapPriorityQueue<SlotInfoElement<PhysicalSlot>>> profileToSlotMap,
            Map<ResourceID, LoadingWeight> taskExecutorsLoad) {
        final ResourceProfile requestProfile = request.getResourceProfile();

        final Set<ResourceProfile> candidateProfiles =
                profileToSlotMap.keySet().stream()
                        .filter(slotProfile -> slotProfile.isMatching(requestProfile))
                        .collect(Collectors.toSet());

        return candidateProfiles.stream()
                .map(
                        candidateProfile -> {
                            HeapPriorityQueue<SlotInfoElement<PhysicalSlot>> slots =
                                    profileToSlotMap.get(candidateProfile);
                            return Objects.isNull(slots) ? null : slots.peek();
                        })
                .filter(Objects::nonNull)
                .min(new SlotInfoElementComparator<>(taskExecutorsLoad));
    }

    private void updateReferenceAfterMatching(
            Map<ResourceProfile, HeapPriorityQueue<SlotInfoElement<PhysicalSlot>>> profileSlots,
            Map<ResourceID, LoadingWeight> taskExecutorsLoad,
            Map<ResourceID, Set<SlotInfoElement<PhysicalSlot>>> taskExecutorSlots,
            SlotInfoElement<PhysicalSlot> targetSlotInfoElement,
            LoadingWeight loading) {
        final ResourceID tmID =
                targetSlotInfoElement.slotInfo.getTaskManagerLocation().getResourceID();
        // Update the loading for the target task executor.
        taskExecutorsLoad.compute(
                tmID,
                (ignoredId, oldLoading) ->
                        Objects.isNull(oldLoading) ? loading : oldLoading.merge(loading));
        // Update the sorted set for slots that is located on the same task executor as targetSlot.
        // Use Map#remove to avoid the ConcurrentModifyException.
        final Set<SlotInfoElement<PhysicalSlot>> slotToReSort = taskExecutorSlots.remove(tmID);
        for (SlotInfoElement<PhysicalSlot> slotEle : slotToReSort) {
            HeapPriorityQueue<SlotInfoElement<PhysicalSlot>> slotsOfProfile =
                    profileSlots.get(slotEle.slotInfo.getResourceProfile());
            // Re-add for the latest order.
            slotsOfProfile.remove(slotEle);
            if (!slotEle.equals(targetSlotInfoElement)) {
                slotsOfProfile.add(slotEle);
            }
        }
        slotToReSort.remove(targetSlotInfoElement);
        taskExecutorSlots.put(tmID, slotToReSort);
    }

    @Override
    public String toString() {
        return TasksBalancedRequestSlotMatchingStrategy.class.getSimpleName();
    }
}
