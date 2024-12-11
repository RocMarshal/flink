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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadInformation;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkState;

/** Simple {@link SlotAssigner} that treats all slots and slot sharing groups equally. */
public class DefaultSlotAssigner implements SlotAssigner {

    @Override
    public Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<PhysicalSlot> freeSlots,
            Collection<ExecutionSlotSharingGroup> requestExecutionSlotSharingGroups,
            JobAllocationsInformation previousAllocations,
            TaskExecutorsLoadInformation taskExecutorsLoadInformation) {

        Iterator<? extends SlotInfo> iterator = freeSlots.iterator();
        Collection<SlotAssignment> assignments = new ArrayList<>();
        for (ExecutionSlotSharingGroup group : requestExecutionSlotSharingGroups) {
            checkState(
                    iterator.hasNext(),
                    "No slots available for group %s (%s more in total). This is likely a bug.",
                    group,
                    requestExecutionSlotSharingGroups.size());
            assignments.add(new SlotAssignment(iterator.next(), group));
        }
        return assignments;
    }
}
