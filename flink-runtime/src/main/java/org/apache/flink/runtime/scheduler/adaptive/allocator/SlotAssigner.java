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
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;

/** The Interface for assigning slots to slot sharing groups. */
@Internal
public interface SlotAssigner {

    Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism,
            JobAllocationsInformation previousAllocations);

    /**
     * Select the target slots to assign with the requested groups.
     *
     * @param slots the raw slots to filter.
     * @param groups the request execution slot sharing groups.
     * @return the target slots that are distributed on the minimal task executors.
     */
    Collection<? extends SlotInfo> selectSlotsInMinimalTaskExecutors(
            Collection<? extends SlotInfo> slots, Collection<ExecutionSlotSharingGroup> groups);

    static Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> getSlotsPerTaskExecutor(
            Collection<? extends SlotInfo> slots) {
        return slots.stream()
                .collect(
                        Collectors.groupingBy(
                                SlotInfo::getTaskManagerLocation,
                                Collectors.mapping(identity(), Collectors.toSet())));
    }
}
