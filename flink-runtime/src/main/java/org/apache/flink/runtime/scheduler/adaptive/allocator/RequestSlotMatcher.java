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

import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadingUtilization;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.adaptive.allocator.StateLocalitySlotAssigner.AllocationScore;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Queue;

/**
 * The interface to define the matcher how to matching the target physical slots with the execution
 * slot sharing groups.
 */
public interface RequestSlotMatcher {

    /** Match slots from the free slots with the given collection of requests execution groups. */
    Collection<SlotAssignment> matchRequestsWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<? extends SlotInfo> freeSlots,
            TaskExecutorsLoadingUtilization taskExecutorsLoadingUtilization,
            @Nonnull Queue<AllocationScore> scores);
}
