/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.loading.DefaultLoadingWeight;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link PreferredAllocationRequestSlotMatchingStrategy}. */
@ExtendWith(TestLoggerExtension.class)
class PreferredAllocationRequestSlotMatchingStrategyTest {

    private final ResourceProfile finedGrainProfile =
            ResourceProfile.newBuilder().setCpuCores(1d).build();

    /**
     * This test ensures that new slots are matched against the preferred allocationIds of the
     * pending requests.
     */
    @Test
    void testNewSlotsAreMatchedAgainstPreferredAllocationIDs() {
        final RequestSlotMatchingStrategy strategy =
                PreferredAllocationRequestSlotMatchingStrategy.create(
                        SimpleRequestSlotMatchingStrategy.INSTANCE);

        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();

        final Collection<TestingPhysicalSlot> slots =
                Arrays.asList(
                        TestingPhysicalSlot.builder().withAllocationID(allocationId1).build(),
                        TestingPhysicalSlot.builder().withAllocationID(allocationId2).build());
        final Collection<PendingRequest> pendingRequests =
                Arrays.asList(
                        PendingRequest.createNormalRequest(
                                new SlotRequestId(),
                                ResourceProfile.UNKNOWN,
                                Collections.singleton(allocationId2)),
                        PendingRequest.createNormalRequest(
                                new SlotRequestId(),
                                ResourceProfile.UNKNOWN,
                                Collections.singleton(allocationId1)));

        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                strategy.matchRequestsAndSlots(slots, pendingRequests, new HashMap<>());

        assertThat(requestSlotMatches).hasSize(2);

        for (RequestSlotMatchingStrategy.RequestSlotMatch requestSlotMatch : requestSlotMatches) {
            assertThat(requestSlotMatch.getPendingRequest().getPreferredAllocations())
                    .contains(requestSlotMatch.getSlot().getAllocationId());
        }
    }

    /**
     * This test ensures that new slots are matched on {@link
     * PreviousAllocationSlotSelectionStrategy} and {@link
     * TasksBalancedRequestSlotMatchingStrategy}.
     */
    @Test
    void testNewSlotsAreMatchedAgainstAllocationAndBalancedPreferredIDs() {
        final RequestSlotMatchingStrategy strategy =
                PreferredAllocationRequestSlotMatchingStrategy.create(
                        TasksBalancedRequestSlotMatchingStrategy.INSTANCE);

        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        TaskManagerLocation tmLocation1 = new LocalTaskManagerLocation();
        TaskManagerLocation tmLocation2 = new LocalTaskManagerLocation();

        final Collection<PendingRequest> pendingRequests =
                createPendingRequests(allocationId2, allocationId1);
        final Collection<TestingPhysicalSlot> slots =
                createSlots(allocationId1, tmLocation1, allocationId2, tmLocation2);

        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                strategy.matchRequestsAndSlots(slots, pendingRequests, new HashMap<>());

        assertThat(requestSlotMatches).hasSize(6);

        checkPreferredAllocationsMatchResult(requestSlotMatches);
    }

    private void checkPreferredAllocationsMatchResult(
            Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches) {
        for (RequestSlotMatchingStrategy.RequestSlotMatch requestSlotMatch : requestSlotMatches) {
            // Check for allocationIds preferred.
            if (!requestSlotMatch.getPendingRequest().getPreferredAllocations().isEmpty()) {
                assertThat(requestSlotMatch.getPendingRequest().getPreferredAllocations())
                        .contains(requestSlotMatch.getSlot().getAllocationId());
            }
        }
    }

    private Collection<TestingPhysicalSlot> createSlots(
            AllocationID allocationId1,
            TaskManagerLocation tmLocation1,
            AllocationID allocationId2,
            TaskManagerLocation tmLocation2) {
        return Arrays.asList(
                createSlots(ResourceProfile.ANY, allocationId1, tmLocation1),
                createSlots(finedGrainProfile, allocationId2, tmLocation2),
                createSlots(tmLocation1),
                createSlots(tmLocation1),
                createSlots(finedGrainProfile, tmLocation1),
                createSlots(finedGrainProfile, tmLocation1),
                createSlots(tmLocation2),
                createSlots(tmLocation2),
                createSlots(finedGrainProfile, tmLocation2),
                createSlots(finedGrainProfile, tmLocation2));
    }

    private Collection<PendingRequest> createPendingRequests(
            AllocationID allocationId2, AllocationID allocationId1) {
        return Arrays.asList(
                createRequest(ResourceProfile.UNKNOWN, 2, allocationId1),
                createRequest(finedGrainProfile, 3, allocationId2),
                createRequest(ResourceProfile.UNKNOWN, 2, null),
                createRequest(ResourceProfile.UNKNOWN, 1, null),
                createRequest(finedGrainProfile, 3, null),
                createRequest(finedGrainProfile, 5, null));
    }

    private PendingRequest createRequest(
            ResourceProfile requestProfile,
            float loading,
            @Nullable AllocationID preferAllocationId) {
        final List<AllocationID> preferAllocationIds =
                Objects.isNull(preferAllocationId)
                        ? Collections.emptyList()
                        : Collections.singletonList(preferAllocationId);
        return PendingRequest.createNormalRequest(
                new SlotRequestId(),
                requestProfile,
                new DefaultLoadingWeight(loading),
                preferAllocationIds);
    }

    private TestingPhysicalSlot createSlots(TaskManagerLocation tmLocation) {
        return createSlots(ResourceProfile.ANY, new AllocationID(), tmLocation);
    }

    private TestingPhysicalSlot createSlots(ResourceProfile resourceProfile, TaskManagerLocation tmLocation) {
        return createSlots(resourceProfile, new AllocationID(), tmLocation);
    }

    private TestingPhysicalSlot createSlots(
            ResourceProfile profile, AllocationID allocationId, TaskManagerLocation tmLocation) {
        return TestingPhysicalSlot.builder()
                .withAllocationID(allocationId)
                .withTaskManagerLocation(tmLocation)
                .withResourceProfile(profile)
                .build();
    }
}
