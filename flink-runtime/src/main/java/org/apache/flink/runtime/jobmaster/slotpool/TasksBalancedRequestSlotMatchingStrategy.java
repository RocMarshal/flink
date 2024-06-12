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
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.scheduler.loading.WeightLoadable;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.CollectionUtil.isNullOrEmpty;

/**
 * The tasks balanced based implementation of {@link RequestSlotMatchingStrategy} that matches the
 * pending requests for tasks balance at task-manager level.
 */
public enum TasksBalancedRequestSlotMatchingStrategy implements RequestSlotMatchingStrategy {
    INSTANCE;

    @Override
    public Collection<RequestSlotMatch> matchRequestsAndSlots(
            Collection<? extends PhysicalSlot> slots,
            Collection<PendingRequest> pendingRequests,
            Map<TaskManagerLocation, LoadingWeight> taskExecutorsLoading) {
        if (pendingRequests.isEmpty()) {
            return Collections.emptyList();
        }

        final Collection<RequestSlotMatch> resultingMatches = new ArrayList<>();
        final List<PendingRequest> sortedRequests = WeightLoadable.sortByLoadingDescend(pendingRequests);
        final Map<ResourceProfile, List<PhysicalSlot>> availableSlotsMap = getSlotCandidatesByResourceProfile(slots);

        for (PendingRequest request : sortedRequests) {
            tryMatchPhysicalSlot(
                    request,
                    availableSlotsMap,
                    taskExecutorsLoading).ifPresent(physicalSlot -> resultingMatches.add(RequestSlotMatch.createFor(request, physicalSlot)));
        }

        return resultingMatches;
    }

    private Optional<PhysicalSlot> tryMatchPhysicalSlot(PendingRequest request,
                                                        Map<ResourceProfile, List<PhysicalSlot>> availableSlotsMap,
                                                        Map<TaskManagerLocation, LoadingWeight> taskExecutorsLoading) {
        final LoadingWeight loading = request.getLoading();
        final ResourceProfile requestProfile = request.getResourceProfile();
        final Set<PhysicalSlot> uniqueMatchedSlots = new HashSet<>();
        final Set<TaskManagerLocation> uniqueTmLocations = new HashSet<>();
        availableSlotsMap.keySet().stream()
                .filter(rp->rp.isMatching(requestProfile))
                .map(rp->availableSlotsMap.getOrDefault(rp, new ArrayList<>())).forEach(new Consumer<List<PhysicalSlot>>() {
                    @Override
                    public void accept(List<PhysicalSlot> physicalSlots) {
                        if (uniqueTmLocations.contains(physicalSlots.g))
                    }
                });
        final Optional<PhysicalSlot> minCandidate = uniqueMatchedSlots
                .stream()
                .min(Comparator.comparing(slot -> taskExecutorsLoading.getOrDefault(
                        slot.getTaskManagerLocation(),
                        LoadingWeight.EMPTY)));
        minCandidate.ifPresent(slot -> {
            taskExecutorsLoading.compute(
                    slot.getTaskManagerLocation(),
                    (taskManagerLocation, oldLoading) -> oldLoading == null ? loading : oldLoading.merge(loading));
            List<PhysicalSlot> physicalSlots = availableSlotsMap.get(slot.getResourceProfile());
            if (physicalSlots == null || physicalSlots.isEmpty()) {
                availableSlotsMap.remove(slot.getResourceProfile());
            } else {
                physicalSlots.remove(slot);
            }
        });
        return minCandidate;
    }


    private Map<ResourceProfile, List<PhysicalSlot>> getSlotCandidatesByResourceProfile(@Nonnull Collection<? extends PhysicalSlot> slots) {
        return slots.stream().collect(Collectors.groupingBy(PhysicalSlot::getResourceProfile, Collectors.toList()));
    }

    @Override
    public String toString() {
        return TasksBalancedRequestSlotMatchingStrategy.class.getSimpleName();
    }
}
