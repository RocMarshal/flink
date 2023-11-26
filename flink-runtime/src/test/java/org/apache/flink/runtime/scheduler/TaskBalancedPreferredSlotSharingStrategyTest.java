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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupImpl;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava32.com.google.common.collect.Sets;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TaskBalancedPreferredSlotSharingStrategy}. */
class TaskBalancedPreferredSlotSharingStrategyTest extends AbstractSlotSharingStrategyTest {

    private SlotSharingStrategy strategy;

    @Override
    protected SlotSharingStrategy getSlotSharingStrategy(
            SchedulingTopology topology,
            Set<SlotSharingGroup> slotSharingGroups,
            Set<CoLocationGroup> coLocationGroups) {
        return new TaskBalancedPreferredSlotSharingStrategy(
                topology, slotSharingGroups, coLocationGroups);
    }

    @Test
    void testVerticesInDifferentSlotSharingGroups() {
        List<Tuple2<JobVertexID, List<TestingSchedulingExecutionVertex>>> jobVertexInfos =
                new ArrayList<>();
        SlotSharingGroup slotSharingGroup1 = new SlotSharingGroup();
        SlotSharingGroup slotSharingGroup2 = new SlotSharingGroup();
        List<TestingJobVertexInfo> testingJobVertexInfos =
                Lists.newArrayList(
                        new TestingJobVertexInfo(1, slotSharingGroup1, null),
                        new TestingJobVertexInfo(2, slotSharingGroup1, null),
                        new TestingJobVertexInfo(3, slotSharingGroup1, null),
                        new TestingJobVertexInfo(1, slotSharingGroup2, null),
                        new TestingJobVertexInfo(2, slotSharingGroup2, null),
                        new TestingJobVertexInfo(2, slotSharingGroup2, null));

        setupCase(testingJobVertexInfos, jobVertexInfos);
        slotSharingGroups = Sets.newHashSet(slotSharingGroup1, slotSharingGroup2);

        final SlotSharingStrategy strategy =
                getSlotSharingStrategy(topology, slotSharingGroups, coLocationGroups);
        assertThat(strategy.getExecutionSlotSharingGroups()).hasSize(5);
        checkBalanceAtSlotsLevelWithoutCoLocation(strategy);

        List<TestingSchedulingExecutionVertex> executionVertices4 = jobVertexInfos.get(4).f1;
        List<TestingSchedulingExecutionVertex> executionVertices5 = jobVertexInfos.get(5).f1;
        assertThat(executionVertices4).hasSameSizeAs(executionVertices5);
        // Check for JVs whose parallelism is the max in the same slot sharing group.
        for (int i = 0; i < executionVertices4.size(); i++) {
            TestingSchedulingExecutionVertex executionVertex4 = executionVertices4.get(i);
            assertThat(strategy.getExecutionSlotSharingGroup(executionVertex4.getId()))
                    .isEqualTo(
                            strategy.getExecutionSlotSharingGroup(
                                    executionVertices5.get(i).getId()));
        }
    }

    private void checkBalanceAtSlotsLevelWithoutCoLocation(SlotSharingStrategy strategy) {
        strategy.getExecutionSlotSharingGroups().stream()
                .collect(Collectors.groupingBy(ExecutionSlotSharingGroup::getSlotSharingGroup))
                .forEach(
                        (slotSharingGroup, executionSlotSharingGroups) -> {
                            Optional<Integer> max =
                                    executionSlotSharingGroups.stream()
                                            .map(
                                                    executionSlotSharingGroup ->
                                                            executionSlotSharingGroup
                                                                    .getExecutionVertexIds()
                                                                    .size())
                                            .max(Comparator.comparing(i -> i));
                            Optional<Integer> min =
                                    executionSlotSharingGroups.stream()
                                            .map(
                                                    executionSlotSharingGroup ->
                                                            executionSlotSharingGroup
                                                                    .getExecutionVertexIds()
                                                                    .size())
                                            .min(Comparator.comparing(i -> i));
                            assertThat(max.get()).isCloseTo(min.get(), Offset.offset(1));
                        });
    }

    @Test
    void testCoLocationConstraintIsRespected() {
        List<Tuple2<JobVertexID, List<TestingSchedulingExecutionVertex>>> jobVertexInfos =
                new ArrayList<>();
        CoLocationGroup coLocationGroup1 = new CoLocationGroupImpl();
        CoLocationGroup coLocationGroup2 = new CoLocationGroupImpl();
        TestingJobVertexInfo tJv0 = new TestingJobVertexInfo(1, slotsharingGroup, null);
        TestingJobVertexInfo tJv1 = new TestingJobVertexInfo(2, slotsharingGroup, coLocationGroup1);
        TestingJobVertexInfo tJv2 = new TestingJobVertexInfo(2, slotsharingGroup, coLocationGroup1);
        TestingJobVertexInfo tJv3 = new TestingJobVertexInfo(1, slotsharingGroup, null);
        TestingJobVertexInfo tJv4 = new TestingJobVertexInfo(4, slotsharingGroup, coLocationGroup1);
        TestingJobVertexInfo tJv5 = new TestingJobVertexInfo(4, slotsharingGroup, coLocationGroup1);
        TestingJobVertexInfo tJv6 = new TestingJobVertexInfo(3, slotsharingGroup, coLocationGroup2);
        List<TestingJobVertexInfo> mockedJobVertices =
                Lists.newArrayList(tJv0, tJv1, tJv2, tJv3, tJv4, tJv5, tJv6);
        setupCase(mockedJobVertices, jobVertexInfos);
        coLocationGroups.add(coLocationGroup1);
        coLocationGroups.add(coLocationGroup2);
        strategy = getSlotSharingStrategy(topology, slotSharingGroups, coLocationGroups);

        List<TestingSchedulingExecutionVertex> jv0Vertices = jobVertexInfos.get(0).f1;
        List<TestingSchedulingExecutionVertex> jv1Vertices = jobVertexInfos.get(1).f1;
        List<TestingSchedulingExecutionVertex> jv2Vertices = jobVertexInfos.get(2).f1;
        List<TestingSchedulingExecutionVertex> jv4Vertices = jobVertexInfos.get(4).f1;
        List<TestingSchedulingExecutionVertex> jv5Vertices = jobVertexInfos.get(5).f1;
        List<TestingSchedulingExecutionVertex> jv6Vertices = jobVertexInfos.get(6).f1;
        // Check vertices of jv1 & jv2
        for (int i = 0; i < jv1Vertices.size(); i++) {
            assertThat(getTargetGroup(jv1Vertices, i))
                    .isEqualTo(getTargetGroup(jv2Vertices, i))
                    .isEqualTo(getTargetGroup(jv4Vertices, i))
                    .isEqualTo(getTargetGroup(jv5Vertices, i));
        }
        // Check vertices of jv4 & jv5
        for (int i = 0; i < jv4Vertices.size(); i++) {
            assertThat(getTargetGroup(jv4Vertices, i)).isEqualTo(getTargetGroup(jv5Vertices, i));
        }
        // Check for tJv4
        assertThat(getTargetGroup(jv4Vertices, 2))
                .isNotEqualTo(getTargetGroup(jv1Vertices, 0))
                .isNotEqualTo(getTargetGroup(jv1Vertices, 1));
        assertThat(getTargetGroup(jv4Vertices, 3))
                .isNotEqualTo(getTargetGroup(jv1Vertices, 0))
                .isNotEqualTo(getTargetGroup(jv1Vertices, 1));
        // Check for tJv6
        assertThat(getTargetGroup(jv6Vertices, 0)).isEqualTo(getTargetGroup(jv0Vertices, 0));
        assertThat(getTargetGroup(jv6Vertices, 1)).isEqualTo(getTargetGroup(jv4Vertices, 3));
        assertThat(getTargetGroup(jv6Vertices, 2)).isEqualTo(getTargetGroup(jv4Vertices, 0));
    }

    private ExecutionSlotSharingGroup getTargetGroup(
            List<TestingSchedulingExecutionVertex> jvVertices, int index) {
        return strategy.getExecutionSlotSharingGroup(jvVertices.get(index).getId());
    }
}
