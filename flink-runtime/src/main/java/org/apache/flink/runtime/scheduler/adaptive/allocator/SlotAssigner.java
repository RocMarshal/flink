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
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadInformation;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Collection;
import java.util.function.Supplier;

import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;

/** Interface for assigning slots to slot sharing groups. */
@Internal
public interface SlotAssigner {

    Supplier<FlinkRuntimeException> NO_SLOTS_EXCEPTION_GETTER =
            () -> new FlinkRuntimeException("No suitable slots enough.");

    /**
     * Assign slots from the free slots with the given collection of requests execution groups.
     *
     * @param jobInformation The job information.
     * @param freeSlots The available free slots.
     * @param requestExecutionSlotSharingGroups The requested execution slot sharing groups.
     * @param previousAllocations The previous allocation information.
     * @param taskExecutorsLoadInformation The loading information of the task managers.
     * @return The slot assignments result.
     */
    Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<PhysicalSlot> freeSlots,
            Collection<ExecutionSlotSharingGroup> requestExecutionSlotSharingGroups,
            JobAllocationsInformation previousAllocations,
            TaskExecutorsLoadInformation taskExecutorsLoadInformation);
}
