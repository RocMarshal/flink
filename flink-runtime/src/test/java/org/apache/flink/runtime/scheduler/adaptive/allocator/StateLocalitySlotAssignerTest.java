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

import org.apache.flink.configuration.TaskManagerOptions.TaskManagerLoadBalanceMode;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadingUtilization;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobAllocationsInformation.VertexAllocationInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation.VertexInformation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.Preconditions;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link StateLocalitySlotAssigner} test. */
@ExtendWith(ParameterizedTestExtension.class)
class StateLocalitySlotAssignerTest {

    @Parameter private TaskManagerLoadBalanceMode loadBalanceMode;

    @Parameters(name = "loadBalanceMode={0}")
    private static Collection<TaskManagerLoadBalanceMode> getTaskManagerLoadBalanceMode() {
        return Lists.newArrayList(TaskManagerLoadBalanceMode.values());
    }

    @TestTemplate
    void testDownScaleWithUnevenStateSize() {
        int newParallelism = 1;
        VertexInformation vertex = createVertex(newParallelism);
        AllocationID allocationWith100bytes = new AllocationID();
        AllocationID allocationWith200bytes = new AllocationID();

        List<VertexAllocationInformation> allocations =
                Arrays.asList(
                        new VertexAllocationInformation(
                                allocationWith100bytes,
                                vertex.getJobVertexID(),
                                KeyGroupRange.of(0, 99),
                                1),
                        new VertexAllocationInformation(
                                allocationWith200bytes,
                                vertex.getJobVertexID(),
                                KeyGroupRange.of(100, 100),
                                200));

        Collection<SlotAssignment> assignments =
                assign(
                        vertex,
                        Arrays.asList(allocationWith100bytes, allocationWith200bytes),
                        allocations);

        verifyAssignments(assignments, newParallelism, allocationWith200bytes);
    }

    @TestTemplate
    void testSlotsAreNotWasted() {
        VertexInformation vertex = createVertex(2);
        AllocationID alloc1 = new AllocationID();
        AllocationID alloc2 = new AllocationID();

        List<VertexAllocationInformation> allocations =
                Arrays.asList(
                        new VertexAllocationInformation(
                                alloc1, vertex.getJobVertexID(), KeyGroupRange.of(0, 9), 1),
                        new VertexAllocationInformation(
                                alloc2, vertex.getJobVertexID(), KeyGroupRange.of(10, 19), 1));

        assign(vertex, Arrays.asList(alloc1, alloc2), allocations);
    }

    @TestTemplate
    void testUpScaling() {
        final int oldParallelism = 3;
        final int newParallelism = 7;
        final int numFreeSlots = 100;
        final VertexInformation vertex = createVertex(newParallelism);
        final List<AllocationID> allocationIDs = createAllocationIDS(numFreeSlots);

        Iterator<AllocationID> iterator = allocationIDs.iterator();
        List<VertexAllocationInformation> prevAllocations =
                initUpScalePreAllocationInfo(oldParallelism, iterator, vertex);

        Collection<SlotAssignment> assignments = assign(vertex, allocationIDs, prevAllocations);

        verifyAssignments(
                assignments,
                newParallelism,
                prevAllocations.stream()
                        .map(VertexAllocationInformation::getAllocationID)
                        .toArray(AllocationID[]::new));
    }

    @TestTemplate
    void testDownScaling() {
        final int oldParallelism = 5;
        final int newParallelism = 1;
        final int numFreeSlots = 100;
        final VertexInformation vertex = createVertex(newParallelism);
        final List<AllocationID> allocationIDs = createAllocationIDS(numFreeSlots);

        final Iterator<AllocationID> iterator = allocationIDs.iterator();
        final AllocationID biggestAllocation = iterator.next();
        final List<VertexAllocationInformation> prevAllocations =
                initDownScalePreAllocationInfo(biggestAllocation, vertex, oldParallelism, iterator);

        Collection<SlotAssignment> assignments = assign(vertex, allocationIDs, prevAllocations);

        verifyAssignments(assignments, newParallelism, biggestAllocation);
    }

    @MethodSource("getTestingArguments")
    @ParameterizedTest(
            name =
                    "oldParallelism={0}, newParallelism={1}, slotNumOfTaskExecutors={2}, expectedMinimalTaskExecutorNum={3}")
    void testMinimalTaskExecutorsAfterRescale(
            int oldParallelism,
            int newParallelism,
            int[] slotNumOfTaskExecutors,
            int expectedMinimalTaskExecutorNum) {

        Preconditions.checkArgument(newParallelism != oldParallelism);
        final int numFreeSlots = Arrays.stream(slotNumOfTaskExecutors).sum();
        final VertexInformation vertex = createVertex(newParallelism);
        final List<AllocationID> allocationIDs = createAllocationIDS(numFreeSlots);

        // pretend that the 1st (0) subtask had half of key groups ...
        final Iterator<AllocationID> iterator = allocationIDs.iterator();
        final AllocationID biggestAllocation = iterator.next();
        final int diffParallelism = newParallelism - oldParallelism;
        List<VertexAllocationInformation> prevAllocations =
                diffParallelism < 0
                        ? initDownScalePreAllocationInfo(
                                biggestAllocation, vertex, oldParallelism, iterator)
                        : initUpScalePreAllocationInfo(oldParallelism, iterator, vertex);
        Collection<SlotAssignment> assigns =
                assign(
                        TaskManagerLoadBalanceMode.NONE,
                        vertex,
                        allocationIDs,
                        prevAllocations,
                        slotNumOfTaskExecutors);
        assertThat(
                        assigns.stream()
                                .map(
                                        assign ->
                                                assign.getSlotInfo()
                                                        .getTaskManagerLocation()
                                                        .getResourceID())
                                .collect(Collectors.toSet()))
                .hasSize(expectedMinimalTaskExecutorNum);
    }

    private static Stream<Arguments> getTestingArguments() {
        return Stream.of(
                Arguments.of(1, 3, new int[] {2, 2, 2}, 2),
                Arguments.of(3, 1, new int[] {2, 2, 2}, 1),
                Arguments.of(3, 13, new int[] {1, 2, 3, 5, 5, 8}, 2),
                Arguments.of(13, 3, new int[] {1, 2, 3, 5, 5, 8}, 1),
                Arguments.of(13, 11, new int[] {2, 3, 3, 3, 3, 3}, 4),
                Arguments.of(11, 13, new int[] {3, 3, 3, 3, 3, 3}, 5));
    }

    private static List<VertexAllocationInformation> initDownScalePreAllocationInfo(
            AllocationID biggestAllocation,
            VertexInformation vertex,
            int oldParallelism,
            Iterator<AllocationID> iterator) {
        List<VertexAllocationInformation> result = new ArrayList<>();
        final int halfOfKeyGroupRange = vertex.getMaxParallelism() / 2;
        // pretend that the 1st (0) subtask had half of key groups ...
        result.add(
                new VertexAllocationInformation(
                        biggestAllocation,
                        vertex.getJobVertexID(),
                        KeyGroupRange.of(0, halfOfKeyGroupRange - 1),
                        1));

        // and the remaining subtasks had only one key group each
        for (int subtaskIdx = 1; subtaskIdx < oldParallelism; subtaskIdx++) {
            int keyGroup = halfOfKeyGroupRange + subtaskIdx;
            result.add(
                    new VertexAllocationInformation(
                            iterator.next(),
                            vertex.getJobVertexID(),
                            KeyGroupRange.of(keyGroup, keyGroup),
                            1));
        }
        return result;
    }

    private static List<VertexAllocationInformation> initUpScalePreAllocationInfo(
            int oldParallelism, Iterator<AllocationID> iterator, VertexInformation vertex) {
        List<VertexAllocationInformation> result = new ArrayList<>();
        for (int i = 0; i < oldParallelism; i++) {
            result.add(
                    new VertexAllocationInformation(
                            iterator.next(),
                            vertex.getJobVertexID(),
                            KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                                    vertex.getMaxParallelism(), oldParallelism, i),
                            1));
        }
        return result;
    }

    private static void verifyAssignments(
            Collection<SlotAssignment> assignments,
            int expectedSize,
            AllocationID... mustHaveAllocationID) {
        assertThat(assignments).hasSize(expectedSize);
        assertThat(
                        assignments.stream()
                                .map(e -> e.getSlotInfo().getAllocationId())
                                .collect(Collectors.toSet()))
                .contains(mustHaveAllocationID);
    }

    private Collection<SlotAssignment> assign(
            VertexInformation vertexInformation,
            List<AllocationID> allocationIDs,
            List<VertexAllocationInformation> allocations) {
        int[] slotNumOfTaskExecutors = new int[allocationIDs.size()];
        Arrays.fill(slotNumOfTaskExecutors, 1);
        return assign(
                loadBalanceMode,
                vertexInformation,
                allocationIDs,
                allocations,
                slotNumOfTaskExecutors);
    }

    private Collection<SlotAssignment> assign(
            TaskManagerLoadBalanceMode loadBalanceMode,
            VertexInformation vertexInformation,
            List<AllocationID> allocationIDs,
            List<VertexAllocationInformation> allocations,
            int[] slotNumOfTaskExecutors) {

        List<TestingSlot> slots = new ArrayList<>(allocationIDs.size());
        int index = 0;
        for (int slotNum : slotNumOfTaskExecutors) {
            TaskManagerLocation tmLocation = new LocalTaskManagerLocation();
            for (int i = 0; i < slotNum; i++) {
                slots.add(createSlot(allocationIDs.get(index), tmLocation));
                index++;
            }
        }
        return new StateLocalitySlotAssigner(loadBalanceMode)
                .assignSlots(
                        new TestJobInformation(singletonList(vertexInformation)),
                        slots,
                        new VertexParallelism(
                                singletonMap(
                                        vertexInformation.getJobVertexID(),
                                        vertexInformation.getParallelism())),
                        TaskExecutorsLoadingUtilization.EMPTY,
                        new JobAllocationsInformation(
                                singletonMap(vertexInformation.getJobVertexID(), allocations)));
    }

    private static TestingSlot createSlot(
            AllocationID allocationID, TaskManagerLocation tmLocation) {
        return new TestingSlot(allocationID, ResourceProfile.ANY, tmLocation);
    }

    private static VertexInformation createVertex(int parallelism) {
        JobVertexID id = new JobVertexID();
        SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        slotSharingGroup.addVertexToGroup(id);
        return new TestVertexInformation(id, parallelism, slotSharingGroup);
    }

    private static List<AllocationID> createAllocationIDS(int numFreeSlots) {
        return IntStream.range(0, numFreeSlots)
                .mapToObj(i -> new AllocationID())
                .collect(Collectors.toList());
    }
}
