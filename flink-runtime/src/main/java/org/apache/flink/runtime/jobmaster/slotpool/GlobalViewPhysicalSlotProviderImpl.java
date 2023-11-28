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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Implementation with global view for {@link PhysicalSlotProvider}. Note: It's only used for
 * streaming mode now.
 */
public class GlobalViewPhysicalSlotProviderImpl extends PhysicalSlotProviderImpl {

    public GlobalViewPhysicalSlotProviderImpl(
            SlotSelectionStrategy slotSelectionStrategy, SlotPool slotPool) {
        super(slotSelectionStrategy, slotPool);
    }

    @Override
    protected Map<SlotRequestId, Optional<PhysicalSlot>> tryAllocateFromAvailable(
            Collection<PhysicalSlotRequest> slotRequests) {
        Map<SlotRequestId, Optional<PhysicalSlot>> availablePhysicalSlots = new HashMap<>();
        for (PhysicalSlotRequest request : slotRequests) {
            availablePhysicalSlots.put(
                    request.getSlotRequestId(),
                    slotPool.allocateAvailableSlot(request, slotSelectionStrategy));
        }
        return availablePhysicalSlots;
    }
}
