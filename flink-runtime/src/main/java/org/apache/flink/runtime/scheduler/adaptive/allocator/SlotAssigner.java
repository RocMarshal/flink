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
import org.apache.flink.configuration.TaskManagerOptions.TaskManagerLoadBalanceMode;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadingUtilization;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/** Abstract class for assigning slots to slot sharing groups. */
@Internal
public abstract class SlotAssigner {

    protected final TaskManagerLoadBalanceMode loadBalanceMode;
    protected final SlotSharingPolicy slotSharingPolicy;
    protected final SlotsFilter slotsFilter;
    protected final RequestSlotMatcher requestSlotMatcher;

    public SlotAssigner(TaskManagerLoadBalanceMode loadBalanceMode, SlotsFilter slotsFilter) {
        this.loadBalanceMode = Preconditions.checkNotNull(loadBalanceMode);
        this.slotSharingPolicy = getSlotSharingPolicy();
        this.slotsFilter = Preconditions.checkNotNull(slotsFilter);
        this.requestSlotMatcher = getRequestSlotMatcher();
    }

    private SlotSharingPolicy getSlotSharingPolicy() {
        return loadBalanceMode == TaskManagerLoadBalanceMode.TASKS
                ? TaskBalancedSlotSharingPolicy.INSTANCE
                : DefaultSlotSharingPolicy.INSTANCE;
    }

    private RequestSlotMatcher getRequestSlotMatcher() {
        switch (loadBalanceMode) {
            case TASKS:
                return new TaskBalancedRequestSlotMatcher();
            case SLOTS:
                return new EvenlySpreadOutRequestSlotMatcher();
            case NONE:
            default:
                return new DefaultRequestSlotMatcher();
        }
    }

    public abstract Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism,
            TaskExecutorsLoadingUtilization taskExecutorsLoadingUtilization,
            JobAllocationsInformation previousAllocations);
}
