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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotAssigner.AllocationScore;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SlotAssigner}. */
class SlotAssignerTest {

    private static final TaskManagerLocation tml1 = new LocalTaskManagerLocation();
    private static final AllocationID allocationId1OfTml1 = new AllocationID();
    private static final AllocationID allocationId2OfTml1 = new AllocationID();
    private static final AllocationID allocationId3OfTml1 = new AllocationID();
    private static final List<TestingSlot> slotsOfTml1 =
            createSlots(tml1, allocationId1OfTml1, allocationId2OfTml1, allocationId3OfTml1);

    private static final TaskManagerLocation tml2 = new LocalTaskManagerLocation();
    private static final AllocationID allocationId1OfTml2 = new AllocationID();
    private static final AllocationID allocationId2OfTml2 = new AllocationID();
    private static final List<TestingSlot> slotsOfTml2 =
            createSlots(tml2, allocationId1OfTml2, allocationId2OfTml2);

    private static final TaskManagerLocation tml3 = new LocalTaskManagerLocation();
    private static final AllocationID allocationId1OfTml3 = new AllocationID();
    private static final AllocationID allocationId2OfTml3 = new AllocationID();
    private static final List<TestingSlot> slotsOfTml3 =
            createSlots(tml3, allocationId1OfTml3, allocationId2OfTml3);

    /** Helper class. */
    private static class AllocationIdScore {
        AllocationID allocationID;
        long score;

        public AllocationIdScore(AllocationID allocationID, long score) {
            this.allocationID = allocationID;
            this.score = score;
        }

        @Override
        public String toString() {
            return "AllocationIdScore{" + "allocationID=" + allocationID + ", score=" + score + '}';
        }
    }

    private static Stream<Arguments> getTestingParameters() {
        return Stream.of(
                Arguments.of(
                        new StateLocalitySlotAssigner(),
                        5,
                        Arrays.asList(
                                new AllocationIdScore(allocationId1OfTml2, 2),
                                new AllocationIdScore(allocationId1OfTml3, 1)),
                        Arrays.asList(tml1, tml2)),
                Arguments.of(
                        new StateLocalitySlotAssigner(),
                        5,
                        Arrays.asList(
                                new AllocationIdScore(allocationId1OfTml3, 2),
                                new AllocationIdScore(allocationId1OfTml3, 1)),
                        Arrays.asList(tml1, tml3)),
                Arguments.of(
                        new StateLocalitySlotAssigner(),
                        3,
                        Collections.emptyList(),
                        Collections.singletonList(tml1)),
                Arguments.of(
                        new StateLocalitySlotAssigner(),
                        6,
                        Collections.singletonList(new AllocationIdScore(allocationId1OfTml2, 2)),
                        Arrays.asList(tml1, tml2, tml3)),
                Arguments.of(
                        new StateLocalitySlotAssigner(),
                        6,
                        Collections.emptyList(),
                        Arrays.asList(tml1, tml2, tml3)),
                Arguments.of(
                        new DefaultSlotAssigner(),
                        3,
                        Collections.emptyList(),
                        Collections.singletonList(tml1)),
                Arguments.of(
                        new DefaultSlotAssigner(),
                        2,
                        Collections.singletonList(new AllocationIdScore(allocationId1OfTml2, 2)),
                        Collections.singletonList(tml1)));
    }

    @MethodSource("getTestingParameters")
    @ParameterizedTest(
            name = "slotAssigner={0}, group={1}, scoredAllocations={2}, minimalTaskExecutors={3}")
    void testSelectSlotsInMinimalTaskExecutors(
            SlotAssigner slotAssigner,
            int groups,
            List<AllocationIdScore> scoredAllocations,
            List<TaskManagerLocation> minimalTaskExecutors) {
        List<TestingSlot> slots = new ArrayList<>();
        slots.addAll(slotsOfTml1);
        slots.addAll(slotsOfTml2);
        slots.addAll(slotsOfTml3);

        final List<ExecutionSlotSharingGroup> groupsPlaceholders = createGroups(groups);
        Collection<AllocationScore> scores = getAllocationScores(scoredAllocations);
        Set<TaskManagerLocation> keptTaskExecutors =
                slotAssigner.selectSlotsInMinimalTaskExecutors(slots, groupsPlaceholders, scores)
                        .stream()
                        .map(SlotInfo::getTaskManagerLocation)
                        .collect(Collectors.toSet());
        assertThat(minimalTaskExecutors).containsAll(keptTaskExecutors);
    }

    private static Collection<AllocationScore> getAllocationScores(
            List<AllocationIdScore> scoredAllocations) {
        if (scoredAllocations == null) {
            return new ArrayList<>();
        }
        return scoredAllocations.stream()
                .map(
                        allocationIdScore ->
                                new AllocationScore(
                                        UUID.randomUUID().toString(),
                                        allocationIdScore.allocationID,
                                        allocationIdScore.score))
                .collect(Collectors.toList());
    }

    private static List<TestingSlot> createSlots(
            TaskManagerLocation tmLocation, AllocationID... allocationIDs) {
        final List<TestingSlot> result = new ArrayList<>(allocationIDs.length);
        for (AllocationID allocationID : allocationIDs) {
            result.add(new TestingSlot(allocationID, ResourceProfile.ANY, tmLocation));
        }
        return result;
    }

    private static List<ExecutionSlotSharingGroup> createGroups(int num) {
        final List<ExecutionSlotSharingGroup> result = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            result.add(new ExecutionSlotSharingGroup(Collections.emptySet()));
        }
        return result;
    }
}
