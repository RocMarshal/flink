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
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.scheduler.SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.configuration.TaskManagerOptions.NUM_TASK_SLOTS;

/** Factory for {@link SlotSharingExecutionSlotAllocator}. */
public class SlotSharingExecutionSlotAllocatorFactory implements ExecutionSlotAllocatorFactory {
    private final PhysicalSlotProvider slotProvider;

    private final boolean slotWillBeOccupiedIndefinitely;

    private final PhysicalSlotRequestBulkChecker bulkChecker;

    private final Time allocationTimeout;

    private final SlotSharingStrategy.Factory slotSharingStrategyFactory;

    private final TaskSchedulingStrategy taskSchedulingStrategy;

    public SlotSharingExecutionSlotAllocatorFactory(
            PhysicalSlotProvider slotProvider,
            boolean slotWillBeOccupiedIndefinitely,
            PhysicalSlotRequestBulkChecker bulkChecker,
            Time allocationTimeout,
            TaskSchedulingStrategy taskSchedulingStrategy) {
        this(
                slotProvider,
                slotWillBeOccupiedIndefinitely,
                bulkChecker,
                allocationTimeout,
                getSlotSharingStrategy(taskSchedulingStrategy),
                taskSchedulingStrategy);
    }

    SlotSharingExecutionSlotAllocatorFactory(
            PhysicalSlotProvider slotProvider,
            boolean slotWillBeOccupiedIndefinitely,
            PhysicalSlotRequestBulkChecker bulkChecker,
            Time allocationTimeout,
            SlotSharingStrategy.Factory slotSharingStrategyFactory,
            TaskSchedulingStrategy taskSchedulingStrategy) {
        this.slotProvider = slotProvider;
        this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        this.bulkChecker = bulkChecker;
        this.slotSharingStrategyFactory = slotSharingStrategyFactory;
        this.allocationTimeout = allocationTimeout;
        this.taskSchedulingStrategy = Preconditions.checkNotNull(taskSchedulingStrategy);
    }

    @Override
    public ExecutionSlotAllocator createInstance(final ExecutionSlotAllocationContext context) {
        SlotSharingStrategy slotSharingStrategy =
                slotSharingStrategyFactory.create(
                        context.getSchedulingTopology(),
                        context.getLogicalSlotSharingGroups(),
                        context.getCoLocationGroups());
        SyncPreferredLocationsRetriever preferredLocationsRetriever =
                new DefaultSyncPreferredLocationsRetriever(context, context);
        SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory =
                new MergingSharedSlotProfileRetrieverFactory(
                        preferredLocationsRetriever,
                        context::findPriorAllocationId,
                        context::getReservedAllocations);
        return new SlotSharingExecutionSlotAllocator(
                slotProvider,
                slotWillBeOccupiedIndefinitely,
                slotSharingStrategy,
                sharedSlotProfileRetrieverFactory,
                bulkChecker,
                allocationTimeout,
                context::getResourceProfile,
                taskSchedulingStrategy,
                context.getConfiguration().getInteger(NUM_TASK_SLOTS));
    }

    static SlotSharingStrategy.Factory getSlotSharingStrategy(
            TaskSchedulingStrategy taskSchedulingStrategy) {
        if (taskSchedulingStrategy == TaskSchedulingStrategy.BALANCED_PREFERRED) {
            return new BalancedPreferredSlotSharingStrategy.Factory();
        }
        if (taskSchedulingStrategy == TaskSchedulingStrategy.LOCAL_INPUT_PREFERRED) {
            return new LocalInputPreferredSlotSharingStrategy.Factory();
        }
        throw new UnsupportedOperationException(
                String.format("Unsupported task scheduling strategy '%s'", taskSchedulingStrategy));
    }
}
