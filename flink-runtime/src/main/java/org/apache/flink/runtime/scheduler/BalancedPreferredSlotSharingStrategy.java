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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This strategy tries to get a balanced tasks scheduling. Execution vertices, which are belong to
 * the same SlotSharingGroup, tend to be put in the same ExecutionSlotSharingGroup. Co-location
 * constraints are ignored at present.
 */
class BalancedPreferredSlotSharingStrategy extends SlotSharingStrategyBase
        implements SlotSharingStrategy, SchedulingTopologyListener {

    public static final Logger LOG =
            LoggerFactory.getLogger(BalancedPreferredSlotSharingStrategy.class);

    BalancedPreferredSlotSharingStrategy(
            final SchedulingTopology topology,
            final Set<SlotSharingGroup> slotSharingGroups,
            final Set<CoLocationGroup> coLocationGroups) {
        super(topology, slotSharingGroups, coLocationGroups);
    }

    @Override
    protected Map<ExecutionVertexID, ExecutionSlotSharingGroup> computeExecutionToESsgMap(
            SchedulingTopology schedulingTopology) {
        return new BalancedExecutionSlotSharingGroupBuilder(
                        schedulingTopology, this.logicalSlotSharingGroups)
                .build();
    }

    static class Factory implements SlotSharingStrategy.Factory {

        public BalancedPreferredSlotSharingStrategy create(
                final SchedulingTopology topology,
                final Set<SlotSharingGroup> logicalSlotSharingGroups,
                final Set<CoLocationGroup> coLocationGroups) {

            return new BalancedPreferredSlotSharingStrategy(
                    topology, logicalSlotSharingGroups, coLocationGroups);
        }
    }

    /**
     * SlotSharingGroupBuilder class for balanced scheduling strategy. Note: This strategy builder
     * is not effected by the {@link
     * org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint}.
     */
    private static class BalancedExecutionSlotSharingGroupBuilder {

        private final SchedulingTopology topology;

        private final Map<JobVertexID, SlotSharingGroup> slotSharingGroupMap;

        /**
         * Record the number of {@link ExecutionSlotSharingGroup}s for {@link SlotSharingGroup}s.
         */
        private final Map<SlotSharingGroup, List<ExecutionSlotSharingGroup>> ssgToExecutionSSGs;

        /**
         * Record the next round-robin {@link ExecutionSlotSharingGroup} index for {@link
         * SlotSharingGroup}s.
         */
        private final Map<SlotSharingGroup, Integer> slotRoundRobinIndex;

        private final Map<ExecutionVertexID, ExecutionSlotSharingGroup>
                executionSlotSharingGroupMap;

        private BalancedExecutionSlotSharingGroupBuilder(
                final SchedulingTopology topology,
                final Set<SlotSharingGroup> logicalSlotSharingGroups) {

            this.topology = checkNotNull(topology);

            this.ssgToExecutionSSGs = new HashMap<>(logicalSlotSharingGroups.size());
            this.slotRoundRobinIndex = new HashMap<>(logicalSlotSharingGroups.size());
            this.slotSharingGroupMap = new HashMap<>();
            this.executionSlotSharingGroupMap = new HashMap<>();

            for (SlotSharingGroup slotSharingGroup : logicalSlotSharingGroups) {
                for (JobVertexID jobVertexId : slotSharingGroup.getJobVertexIds()) {
                    slotSharingGroupMap.put(jobVertexId, slotSharingGroup);
                }
            }
        }

        private void computeSsgToESsgMap(
                LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> allVertices) {

            allVertices.entrySet().stream()
                    .map(
                            entry ->
                                    new AbstractMap.SimpleEntry<>(
                                            entry.getKey(), entry.getValue().size()))
                    .collect(
                            Collectors.groupingBy(entry -> slotSharingGroupMap.get(entry.getKey())))
                    .forEach(
                            (ssg, entries) -> {
                                Integer slotNumOfSsg =
                                        entries.stream()
                                                .map(Map.Entry::getValue)
                                                .max(Comparator.comparingInt(o -> o))
                                                .orElseThrow(
                                                        () ->
                                                                new FlinkRuntimeException(
                                                                        String.format(
                                                                                "Error state when computing the number of slots in %s",
                                                                                ssg)));
                                List<ExecutionSlotSharingGroup> eSsgs =
                                        new ArrayList<>(slotNumOfSsg);
                                for (int i = 0; i < slotNumOfSsg; i++) {
                                    ExecutionSlotSharingGroup executionSlotSharingGroup =
                                            new ExecutionSlotSharingGroup(ssg, i);
                                    eSsgs.add(i, executionSlotSharingGroup);
                                    LOG.debug(
                                            "Set resourceProfile {} for {}th executionSlotSharingGroup {} in slotSharingGroup {}.",
                                            ssg.getResourceProfile(),
                                            i,
                                            executionSlotSharingGroup,
                                            ssg);
                                }
                                ssgToExecutionSSGs.put(ssg, eSsgs);
                            });
        }

        private Map<ExecutionVertexID, ExecutionSlotSharingGroup> build() {
            final LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> allVertices =
                    getExecutionVertices(topology);

            computeSsgToESsgMap(allVertices);

            // Loop on job vertices
            for (Map.Entry<JobVertexID, List<SchedulingExecutionVertex>> entry :
                    allVertices.entrySet()) {
                SlotSharingGroup ssg = slotSharingGroupMap.get(entry.getKey());
                List<ExecutionSlotSharingGroup> executionSSGs = ssgToExecutionSSGs.get(ssg);
                int parallelismOfJv = entry.getValue().size();
                int index = getSlotRoundRobinIndex(parallelismOfJv, ssg);

                for (SchedulingExecutionVertex sev : entry.getValue()) {
                    executionSSGs.get(index).addVertex(sev.getId());
                    executionSlotSharingGroupMap.put(sev.getId(), executionSSGs.get(index));
                    index = ++index % executionSSGs.size();
                }
                updateSlotRoundRobinIndexIfNeeded(parallelismOfJv, ssg, index);
            }
            return executionSlotSharingGroupMap;
        }

        private int getSlotRoundRobinIndex(int parallelismOfJv, SlotSharingGroup ssg) {
            return parallelismOfJv == ssgToExecutionSSGs.get(ssg).size()
                    ? 0
                    : slotRoundRobinIndex.getOrDefault(ssg, 0);
        }

        private void updateSlotRoundRobinIndexIfNeeded(
                int parallelismOfJv, SlotSharingGroup ssg, int nextIndex) {
            if (parallelismOfJv != ssgToExecutionSSGs.get(ssg).size()) {
                slotRoundRobinIndex.put(ssg, nextIndex);
            }
        }
    }
}
