/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.TaskSchedulingStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.LogLevelExtension;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createRandomExecutionVertexId;
import static org.apache.flink.runtime.scheduler.SlotSharingExecutionSlotAllocatorTest.TestingSharedSlotProfileRetrieverFactory;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link SlotSharingExecutionSlotAllocator} with {@link
 * org.apache.flink.api.common.TaskSchedulingStrategy#BALANCED_PREFERRED}.
 */
@ExtendWith(LogLevelExtension.class)
class BalancedSlotSharingExecutionSlotAllocatorTest {

    private List<ExecutionVertexID> createExecutionVertexIDs(
            JobVertexID jobVertexID, int subtaskNumber, List<ExecutionVertexID> collector) {
        Preconditions.checkState(subtaskNumber > 0);
        List<ExecutionVertexID> executionVertexIDs = new ArrayList<>(subtaskNumber);
        for (int i = 0; i < subtaskNumber; i++) {
            ExecutionVertexID executionVertexId = createRandomExecutionVertexId(jobVertexID, i);
            collector.add(executionVertexId);
            executionVertexIDs.add(i, executionVertexId);
        }
        return executionVertexIDs;
    }

    private SlotSharingGroup getSlotSharingGroup() {
        ResourceProfile resourceProfile = ResourceProfile.newBuilder().build();
        SlotSharingGroup ssg = new SlotSharingGroup();
        ssg.setResourceProfile(resourceProfile);
        return ssg;
    }

    @Test
    void testAllocatePhysicalSlotForNewSharedSlot() {
        SlotSharingGroup ssg1 = getSlotSharingGroup();
        SlotSharingGroup ssg2 = getSlotSharingGroup();

        List<ExecutionVertexID> collector = new ArrayList<>();

        JobVertexID jv0 = new JobVertexID();
        JobVertexID jv1 = new JobVertexID();
        JobVertexID jv2 = new JobVertexID();

        JobVertexID jv3 = new JobVertexID();
        JobVertexID jv4 = new JobVertexID();
        JobVertexID jv5 = new JobVertexID();

        AllocationContext context =
                AllocationContext.newBuilder()
                        .addExecutionVertexIDs(ssg1, createExecutionVertexIDs(jv0, 2, collector))
                        .addExecutionVertexIDs(ssg1, createExecutionVertexIDs(jv1, 3, collector))
                        .addExecutionVertexIDs(ssg1, createExecutionVertexIDs(jv2, 5, collector))
                        .addExecutionVertexIDs(ssg2, createExecutionVertexIDs(jv3, 4, collector))
                        .addExecutionVertexIDs(ssg2, createExecutionVertexIDs(jv4, 4, collector))
                        .addExecutionVertexIDs(ssg2, createExecutionVertexIDs(jv5, 8, collector))
                        .build();

        final Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> sortedMap =
                context.getAllocator().groupByExecutionSlotSharingGroup(collector);
        assertThat(
                        sortedMap.keySet().stream()
                                .map(ExecutionSlotSharingGroup::getIndexInSsg)
                                .collect(Collectors.toList()))
                .containsExactly(0, 3, 1, 4, 2, 0, 3, 6, 1, 4, 7, 2, 5);
    }

    /** Allocation test util class. */
    public static class AllocationContext {
        private final SlotSharingExecutionSlotAllocator allocator;

        private AllocationContext(SlotSharingExecutionSlotAllocator allocator) {
            this.allocator = allocator;
        }

        public SlotSharingExecutionSlotAllocator getAllocator() {
            return allocator;
        }

        public static AllocationContext.Builder newBuilder() {
            return new AllocationContext.Builder();
        }

        /** Builder util. */
        public static class Builder {
            private final Map<SlotSharingGroup, List<List<ExecutionVertexID>>> slotToExecutionIDs =
                    new LinkedHashMap<>();
            private final PhysicalSlotRequestBulkChecker bulkChecker =
                    new TestingPhysicalSlotRequestBulkChecker();

            private final TestingPhysicalSlotProvider physicalSlotProvider =
                    TestingPhysicalSlotProvider.createWithInfiniteSlotCreation();

            public AllocationContext.Builder addExecutionVertexIDs(
                    SlotSharingGroup slotSharingGroup, List<ExecutionVertexID> executionVertexIDs) {
                List<List<ExecutionVertexID>> lists =
                        slotToExecutionIDs.computeIfAbsent(
                                slotSharingGroup, ssg -> new ArrayList<>());
                lists.add(executionVertexIDs);
                return this;
            }

            public AllocationContext build() {
                TestingSharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory =
                        new TestingSharedSlotProfileRetrieverFactory();
                TestingSlotSharingStrategy slotSharingStrategy =
                        TestingSlotSharingStrategy.createWithGroupsAndResources(slotToExecutionIDs);
                boolean slotWillBeOccupiedIndefinitely = true;
                SlotSharingExecutionSlotAllocator allocator =
                        new SlotSharingExecutionSlotAllocator(
                                physicalSlotProvider,
                                slotWillBeOccupiedIndefinitely,
                                slotSharingStrategy,
                                sharedSlotProfileRetrieverFactory,
                                bulkChecker,
                                Time.milliseconds(100L),
                                executionVertexID ->
                                        slotSharingStrategy
                                                .getExecutionSlotSharingGroup(executionVertexID)
                                                .getResourceProfile(),
                                TaskSchedulingStrategy.BALANCED_PREFERRED,
                                2);
                return new AllocationContext(allocator);
            }
        }
    }

    private static class TestingSlotSharingStrategy implements SlotSharingStrategy {
        private final Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroups;

        private TestingSlotSharingStrategy(
                Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroups) {
            this.executionSlotSharingGroups = executionSlotSharingGroups;
        }

        @Override
        public ExecutionSlotSharingGroup getExecutionSlotSharingGroup(
                ExecutionVertexID executionVertexId) {
            return executionSlotSharingGroups.get(executionVertexId);
        }

        @Override
        public Set<ExecutionSlotSharingGroup> getExecutionSlotSharingGroups() {
            return new HashSet<>(executionSlotSharingGroups.values());
        }

        private static TestingSlotSharingStrategy createWithGroupsAndResources(
                Map<SlotSharingGroup, List<List<ExecutionVertexID>>> slotToExecutionIDs) {

            Map<ExecutionVertexID, ExecutionSlotSharingGroup> eSsgGroups = new HashMap<>();

            for (Map.Entry<SlotSharingGroup, List<List<ExecutionVertexID>>> slotExecutionVertexIDs :
                    slotToExecutionIDs.entrySet()) {
                Integer slotNumOfSsg =
                        slotExecutionVertexIDs.getValue().stream()
                                .map(List::size)
                                .max(Comparator.comparingInt(o -> o))
                                .orElseThrow(RuntimeException::new);
                ExecutionSlotSharingGroup[] eSsgArray = new ExecutionSlotSharingGroup[slotNumOfSsg];

                for (int indexOfSsg = 0; indexOfSsg < slotNumOfSsg; indexOfSsg++) {
                    ExecutionSlotSharingGroup eSsg =
                            new ExecutionSlotSharingGroup(
                                    slotExecutionVertexIDs.getKey(), indexOfSsg);
                    eSsgArray[indexOfSsg] = eSsg;
                }

                int slotIndex = -1;
                for (List<ExecutionVertexID> executionVertexIds :
                        slotExecutionVertexIDs.getValue()) {
                    int parallel = executionVertexIds.size();
                    for (int j = 0; j < parallel; j++) {
                        slotIndex = parallel == slotNumOfSsg ? j : ++slotIndex % slotNumOfSsg;
                        eSsgArray[slotIndex].addVertex(executionVertexIds.get(j));
                        eSsgGroups.put(executionVertexIds.get(j), eSsgArray[slotIndex]);
                    }
                }
            }

            return new TestingSlotSharingStrategy(eSsgGroups);
        }
    }
}
