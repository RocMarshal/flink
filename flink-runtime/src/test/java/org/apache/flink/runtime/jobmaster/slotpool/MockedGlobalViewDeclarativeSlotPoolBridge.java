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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/** Mocked from {@link GlobalViewDeclarativeSlotPoolBridge} for testing. */
class MockedGlobalViewDeclarativeSlotPoolBridge extends GlobalViewDeclarativeSlotPoolBridge {
    public MockedGlobalViewDeclarativeSlotPoolBridge(
            JobID jobId,
            DeclarativeSlotPoolFactory declarativeSlotPoolFactory,
            Clock clock,
            Time rpcTimeout,
            Time idleSlotTimeout,
            Time batchSlotTimeout,
            Time slotRequestMaxInterval,
            RequestSlotMatchingStrategy requestSlotMatchingStrategy) {
        super(
                jobId,
                declarativeSlotPoolFactory,
                clock,
                rpcTimeout,
                idleSlotTimeout,
                batchSlotTimeout,
                slotRequestMaxInterval,
                requestSlotMatchingStrategy);
    }

    @Override
    void newSlotsAreAvailable(Collection<? extends PhysicalSlot> newSlots) {
        getReceivedNewSlots().addAll(newSlots);
        if (newSlots.isEmpty() && getReceivedNewSlots().isEmpty()) {
            tryFulfillWithAlreadyAvailableSlots();
        } else {
            tryFulfillWithAlreadyAvailableSlots();
            tryFulfillWithNewAvailableSlots(newSlots);
        }
    }

    private void tryFulfillWithAlreadyAvailableSlots() {
        Map<SlotRequestId, PhysicalSlot> preFulfilledFromAvailableSlots =
                getPreFulfilledFromAvailableSlots();
        for (SlotRequestId preFulFilledReqId : preFulfilledFromAvailableSlots.keySet()) {
            PhysicalSlot slot = preFulfilledFromAvailableSlots.get(preFulFilledReqId);
            PendingRequest pendingRequest = pendingRequests.get(preFulFilledReqId);
            if (pendingRequest == null) {
                continue;
            }
            pendingRequest.fulfill(slot);
            pendingRequests.remove(preFulFilledReqId);
            getIncreasedResourceRequirements().remove(preFulFilledReqId);
            preFulfilledFromAvailableSlots.remove(preFulFilledReqId);
        }
        Preconditions.checkState(preFulfilledFromAvailableSlots.isEmpty());
    }

    private void tryFulfillWithNewAvailableSlots(Collection<? extends PhysicalSlot> newSlots) {
        Set<PhysicalSlot> receivedNewSlots = getReceivedNewSlots();
        receivedNewSlots.addAll(newSlots);

        if (receivedNewSlots.size() < pendingRequests.size()) {
            return;
        }

        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                requestSlotMatchingStrategy.matchRequestsAndSlots(
                        receivedNewSlots, pendingRequests.values());

        if (requestSlotMatches.size() < pendingRequests.size()) {
            return;
        }

        reserveAndFulfillSlots(requestSlotMatches);
        requestSlotMatches.forEach(
                requestSlotMatch -> {
                    pendingRequests.remove(requestSlotMatch.getPendingRequest().getSlotRequestId());
                    getIncreasedResourceRequirements()
                            .remove(requestSlotMatch.getPendingRequest().getSlotRequestId());
                });
        receivedNewSlots.clear();
    }
}
