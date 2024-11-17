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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.jobmaster.SlotRequestId;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Task balanced physical slot provider. */
public class TaskBalancedPhysicalSlotProviderImpl extends PhysicalSlotProviderImpl {

    public TaskBalancedPhysicalSlotProviderImpl(
            SlotSelectionStrategy slotSelectionStrategy, SlotPool slotPool) {
        super(slotSelectionStrategy, slotPool);
    }

    @Override
    public Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>> allocatePhysicalSlots(
            Collection<PhysicalSlotRequest> physicalSlotRequests) {

        logRequestInfo(physicalSlotRequests);

        Map<SlotRequestId, PhysicalSlotRequest> physicalSlotRequestsById =
                physicalSlotRequests.stream()
                        .collect(
                                Collectors.toMap(
                                        PhysicalSlotRequest::getSlotRequestId,
                                        Function.identity()));

        return physicalSlotRequestsById.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> {
                                    SlotRequestId slotRequestId = entry.getKey();
                                    PhysicalSlotRequest physicalSlotRequest = entry.getValue();
                                    return requestNewSlot(
                                                    slotRequestId,
                                                    physicalSlotRequest
                                                            .getPhysicalSlotLoadableResourceProfile(),
                                                    physicalSlotRequest
                                                            .getSlotProfile()
                                                            .getPreferredAllocations(),
                                                    physicalSlotRequest
                                                            .willSlotBeOccupiedIndefinitely())
                                            .thenApply(
                                                    physicalSlot ->
                                                            new PhysicalSlotRequest.Result(
                                                                    slotRequestId, physicalSlot));
                                }));
    }
}
