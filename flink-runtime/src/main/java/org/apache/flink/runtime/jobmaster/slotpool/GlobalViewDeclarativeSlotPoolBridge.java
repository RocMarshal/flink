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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskexecutor.slot.DefaultTimerService;
import org.apache.flink.runtime.taskexecutor.slot.TimeoutListener;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * {@link SlotPool} implementation which could use the {@link GlobalViewDeclarativeSlotPoolBridge}
 * to allocate slots in a global view. Note: It's only used for streaming mode now.
 */
public class GlobalViewDeclarativeSlotPoolBridge extends DeclarativeSlotPoolBridge
        implements TimeoutListener<GlobalViewDeclarativeSlotPoolBridge> {

    public static final Logger LOG =
            LoggerFactory.getLogger(GlobalViewDeclarativeSlotPoolBridge.class);
    private final Map<SlotRequestId, Boolean> increasedResourceRequirements;

    private final TimerService<GlobalViewDeclarativeSlotPoolBridge> timerService;

    private final @Nonnull Set<PhysicalSlot> receivedNewSlots;

    private final @Nonnull Map<SlotRequestId, PhysicalSlot> preFulfilledFromAvailableSlots;
    private final Time slotRequestMaxInterval;

    public GlobalViewDeclarativeSlotPoolBridge(
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
                requestSlotMatchingStrategy);
        this.slotRequestMaxInterval = Preconditions.checkNotNull(slotRequestMaxInterval);
        this.receivedNewSlots = new HashSet<>();
        this.preFulfilledFromAvailableSlots = new HashMap<>();
        this.increasedResourceRequirements = new HashMap<>();
        this.timerService =
                new DefaultTimerService<>(
                        new ScheduledThreadPoolExecutor(1),
                        slotRequestMaxInterval.toMilliseconds());
    }

    @Override
    protected void internalRequestNewAllocatedSlot(PendingRequest pendingRequest) {
        pendingRequests.put(pendingRequest.getSlotRequestId(), pendingRequest);
        increasedResourceRequirements.put(pendingRequest.getSlotRequestId(), false);

        timerService.registerTimeout(
                this, slotRequestMaxInterval.getSize(), slotRequestMaxInterval.getUnit());
    }

    @Override
    void newSlotsAreAvailable(Collection<? extends PhysicalSlot> newSlots) {
        receivedNewSlots.addAll(newSlots);
        if (newSlots.isEmpty() && receivedNewSlots.isEmpty()) {
            // TODO: Do the matching logic only for available slots.
        } else {
            // TODO: Do the matching logic for new slots and available slots.
        }
    }

    @Override
    public void start(
            JobMasterId jobMasterId, String address, ComponentMainThreadExecutor mainThreadExecutor)
            throws Exception {
        super.start(jobMasterId, address, mainThreadExecutor);
        this.timerService.start(this);
    }

    @Override
    public void releaseSlot(@Nonnull SlotRequestId slotRequestId, @Nullable Throwable cause) {
        log.debug("Release slot with slot request id {}", slotRequestId);
        assertRunningInMainThread();

        final PendingRequest pendingRequest = pendingRequests.remove(slotRequestId);

        if (pendingRequest != null) {
            if (increasedResourceRequirements.getOrDefault(
                    pendingRequest.getSlotRequestId(), false)) {
                getDeclarativeSlotPool()
                        .decreaseResourceRequirementsBy(
                                ResourceCounter.withResource(
                                        pendingRequest.getResourceProfile(), 1));
                increasedResourceRequirements.remove(pendingRequest.getSlotRequestId());
            }

            pendingRequest.failRequest(
                    new FlinkException(
                            String.format(
                                    "Pending slot request with %s has been released.",
                                    pendingRequest.getSlotRequestId()),
                            cause));
        } else {
            final AllocationID allocationId = fulfilledRequests.remove(slotRequestId);

            if (allocationId != null) {
                ResourceCounter previouslyFulfilledRequirement =
                        getDeclarativeSlotPool()
                                .freeReservedSlot(allocationId, cause, getRelativeTimeMillis());
                getDeclarativeSlotPool()
                        .decreaseResourceRequirementsBy(previouslyFulfilledRequirement);
            } else {
                log.debug(
                        "Could not find slot which has fulfilled slot request {}. Ignoring the release operation.",
                        slotRequestId);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        timerService.stop();
    }

    @Override
    public void notifyTimeout(GlobalViewDeclarativeSlotPoolBridge key, UUID ticket) {
        if (!timerService.isValid(key, ticket)) {
            LOG.warn("Invalid timeout trigger for {} of {}", ticket, key);
            return;
        }
        ResourceCounter rc = computeResourceCounterNeeded();
        if (!rc.isEmpty()) {
            getDeclarativeSlotPool().increaseResourceRequirementsBy(rc);
            increasedResourceRequirements.replaceAll((slotRequestId, oldValue) -> true);
        } else {
            newSlotsAreAvailable(Collections.emptySet());
        }
    }

    private ResourceCounter computeResourceCounterNeeded() {
        ResourceCounter result = ResourceCounter.empty();
        for (SlotRequestId slotRequestId : increasedResourceRequirements.keySet()) {
            boolean increased = increasedResourceRequirements.get(slotRequestId);
            PendingRequest pendingRequest = pendingRequests.get(slotRequestId);
            Preconditions.checkNotNull(pendingRequest);
            if (!increased) {
                result =
                        result.add(
                                ResourceCounter.withResource(
                                        pendingRequest.getResourceProfile(), 1));
            }
        }
        return result;
    }

    @Override
    public Optional<PhysicalSlot> allocateAvailableSlot(
            PhysicalSlotRequest physicalSlotRequest, SlotSelectionStrategy slotSelectionStrategy) {
        super.assertRunningInMainThread();

        timerService.registerTimeout(
                this, slotRequestMaxInterval.getSize(), slotRequestMaxInterval.getUnit());

        FreeSlotInfoTracker freeSlotInfoTracker = getDeclarativeSlotPool().getFreeSlotInfoTracker();
        if (freeSlotInfoTracker.getFreeSlotsInformation().isEmpty()) {
            return Optional.empty();
        }

        SlotRequestId slotRequestId = physicalSlotRequest.getSlotRequestId();
        SlotProfile slotProfile = physicalSlotRequest.getSlotProfile();
        ResourceProfile requirementProfile = slotProfile.getPhysicalSlotResourceProfile();
        Preconditions.checkNotNull(requirementProfile, "The requiredSlotProfile must not be null.");

        Optional<SlotSelectionStrategy.SlotInfoAndLocality> slotInfoAndLocalityOpt =
                slotSelectionStrategy.selectBestSlotForProfile(freeSlotInfoTracker, slotProfile);

        if (!slotInfoAndLocalityOpt.isPresent()) {
            return Optional.empty();
        }

        AllocationID allocationID = slotInfoAndLocalityOpt.get().getSlotInfo().getAllocationId();
        getDeclarativeSlotPool()
                .increaseResourceRequirementsBy(
                        ResourceCounter.withResource(requirementProfile, 1));
        increasedResourceRequirements.put(slotRequestId, true);
        pendingRequests.put(
                slotRequestId,
                PendingRequest.createNormalRequest(
                        slotRequestId,
                        requirementProfile,
                        Collections.singletonList(allocationID)));

        final PhysicalSlot physicalSlot =
                getDeclarativeSlotPool().reserveFreeSlot(allocationID, requirementProfile);
        preFulfilledFromAvailableSlots.put(slotRequestId, physicalSlot);

        return Optional.empty();
    }

    @VisibleForTesting
    public Map<SlotRequestId, PhysicalSlot> getPreFulfilledFromAvailableSlots() {
        return preFulfilledFromAvailableSlots;
    }

    @VisibleForTesting
    public Map<SlotRequestId, Boolean> getIncreasedResourceRequirements() {
        return increasedResourceRequirements;
    }

    @VisibleForTesting
    public Set<PhysicalSlot> getReceivedNewSlots() {
        return receivedNewSlots;
    }
}
