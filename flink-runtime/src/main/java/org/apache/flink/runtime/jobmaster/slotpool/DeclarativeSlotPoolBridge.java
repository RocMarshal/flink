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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.LoadableResourceProfile;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** {@link SlotPool} implementation which uses the {@link DeclarativeSlotPool} to allocate slots. */
public class DeclarativeSlotPoolBridge extends DeclarativeSlotPoolService implements SlotPool {
    public static final Logger LOG = LoggerFactory.getLogger(DeclarativeSlotPoolBridge.class);

    private final Map<SlotRequestId, PendingRequest> pendingRequests;
    private final Map<SlotRequestId, AllocationID> fulfilledRequests;
    private final Time idleSlotTimeout;

    @Override
    public String toString() {
        return "DeclarativeSlotPoolBridge{"
                + "pendingRequests="
                + pendingRequests
                + ", fulfilledRequests="
                + fulfilledRequests
                + ", idleSlotTimeout="
                + idleSlotTimeout
                + ", requestSlotMatchingStrategy="
                + requestSlotMatchingStrategy
                + ", componentMainThreadExecutor="
                + componentMainThreadExecutor
                + ", batchSlotTimeout="
                + batchSlotTimeout
                + ", isBatchSlotRequestTimeoutCheckDisabled="
                + isBatchSlotRequestTimeoutCheckDisabled
                + ", isJobRestarting="
                + isJobRestarting
                + ", slotBatchAllocatable="
                + slotBatchAllocatable
                + ", receivedSlots="
                + receivedSlots
                + ", declarativeSlotPoolService="
                + super.toString()
                + '}';
    }

    private final RequestSlotMatchingStrategy requestSlotMatchingStrategy;

    @Nullable private ComponentMainThreadExecutor componentMainThreadExecutor;

    private final Time batchSlotTimeout;
    private boolean isBatchSlotRequestTimeoutCheckDisabled;

    private boolean isJobRestarting = false;

    private final boolean slotBatchAllocatable;
    private final Set<PhysicalSlot> receivedSlots;

    public DeclarativeSlotPoolBridge(
            JobID jobId,
            DeclarativeSlotPoolFactory declarativeSlotPoolFactory,
            Clock clock,
            Time rpcTimeout,
            Time idleSlotTimeout,
            Time batchSlotTimeout,
            RequestSlotMatchingStrategy requestSlotMatchingStrategy,
            Duration slotRequestMaxInterval,
            boolean slotBatchAllocatable,
            @Nonnull ComponentMainThreadExecutor componentMainThreadExecutor) {
        super(
                jobId,
                declarativeSlotPoolFactory,
                clock,
                idleSlotTimeout,
                rpcTimeout,
                slotRequestMaxInterval,
                componentMainThreadExecutor);

        this.idleSlotTimeout = idleSlotTimeout;
        this.batchSlotTimeout = Preconditions.checkNotNull(batchSlotTimeout);

        log.debug(
                "Using the request slot matching strategy: {}",
                requestSlotMatchingStrategy.getClass().getSimpleName());
        this.requestSlotMatchingStrategy = requestSlotMatchingStrategy;

        this.isBatchSlotRequestTimeoutCheckDisabled = false;

        this.pendingRequests = new LinkedHashMap<>();
        this.fulfilledRequests = new HashMap<>();
        this.slotBatchAllocatable = slotBatchAllocatable;
        this.receivedSlots = new HashSet<>();
        new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                while (true) {
                                    try {
                                        Thread.sleep(500L);
                                        LOG.info("__debug: {}", DeclarativeSlotPoolBridge.this);
                                    } catch (InterruptedException e) {
                                        System.err.println("Error in debug." + e.getMessage());
                                    }
                                }
                            }
                        })
                .start();
    }

    @Override
    public <T> Optional<T> castInto(Class<T> clazz) {
        if (clazz.isAssignableFrom(getClass())) {
            return Optional.of(clazz.cast(this));
        }

        return Optional.empty();
    }

    @Override
    protected void onStart(ComponentMainThreadExecutor componentMainThreadExecutor) {
        this.componentMainThreadExecutor = componentMainThreadExecutor;

        getDeclarativeSlotPool().registerNewSlotsListener(this::newSlotsAreAvailable);

        componentMainThreadExecutor.schedule(
                this::checkIdleSlotTimeout,
                idleSlotTimeout.toMilliseconds(),
                TimeUnit.MILLISECONDS);
        componentMainThreadExecutor.schedule(
                this::checkBatchSlotTimeout,
                batchSlotTimeout.toMilliseconds(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    protected void onClose() {
        final FlinkException cause = new FlinkException("Closing slot pool");
        cancelPendingRequests(request -> true, cause);
    }

    /**
     * To set whether the underlying is currently restarting or not. In the former case the slot
     * pool bridge will accept all incoming slot offers.
     *
     * @param isJobRestarting whether this is restarting or not
     */
    @Override
    public void setIsJobRestarting(boolean isJobRestarting) {
        this.isJobRestarting = isJobRestarting;
    }

    @Override
    public Collection<SlotOffer> offerSlots(
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Collection<SlotOffer> offers) {
        assertHasBeenStarted();

        if (!isTaskManagerRegistered(taskManagerLocation.getResourceID())) {
            log.debug(
                    "Ignoring offered slots from unknown task manager {}.",
                    taskManagerLocation.getResourceID());
            return Collections.emptyList();
        }

        if (isJobRestarting) {
            return getDeclarativeSlotPool()
                    .registerSlots(
                            offers,
                            taskManagerLocation,
                            taskManagerGateway,
                            getRelativeTimeMillis());

        } else {
            return getDeclarativeSlotPool()
                    .offerSlots(
                            offers,
                            taskManagerLocation,
                            taskManagerGateway,
                            getRelativeTimeMillis());
        }
    }

    private void cancelPendingRequests(
            Predicate<PendingRequest> requestPredicate, FlinkException cancelCause) {

        ResourceCounter decreasedResourceRequirements = ResourceCounter.empty();

        // need a copy since failing a request could trigger another request to be issued
        final Iterable<PendingRequest> pendingRequestsToFail =
                new ArrayList<>(pendingRequests.values());
        pendingRequests.clear();

        for (PendingRequest pendingRequest : pendingRequestsToFail) {
            if (requestPredicate.test(pendingRequest)) {
                pendingRequest.failRequest(cancelCause);
                decreasedResourceRequirements =
                        decreasedResourceRequirements.add(
                                pendingRequest.getLoadableResourceProfile());
            } else {
                pendingRequests.put(pendingRequest.getSlotRequestId(), pendingRequest);
            }
        }

        getDeclarativeSlotPool().decreaseResourceRequirementsBy(decreasedResourceRequirements);
    }

    @Override
    protected void onReleaseTaskManager(ResourceCounter previouslyFulfilledRequirement) {
        getDeclarativeSlotPool().decreaseResourceRequirementsBy(previouslyFulfilledRequirement);
    }

    @VisibleForTesting
    void newSlotsAreAvailable(Collection<? extends PhysicalSlot> newSlots) {
        if (!slotBatchAllocatable) {
            final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                    requestSlotMatchingStrategy.matchRequestsAndSlots(
                            newSlots,
                            pendingRequests.values(),
                            getDeclarativeSlotPool().getTaskExecutorsLoadingWeight(),
                            getDeclarativeSlotPool().getPreferredResourceProfileCounter());
            reserveMatchedFreeSlots(requestSlotMatches);
            fulfillMatchedSlots(requestSlotMatches);
            return;
        }

        receivedSlots.addAll(newSlots);
        log.debug("Received new available slots: {}, receivedSlots: {}", newSlots, receivedSlots);

        if (receivedSlots.size() < pendingRequests.size()) {
            return;
        }
        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                requestSlotMatchingStrategy.matchRequestsAndSlots(
                        receivedSlots,
                        pendingRequests.values(),
                        getDeclarativeSlotPool().getTaskExecutorsLoadingWeight(),
                        getDeclarativeSlotPool().getPreferredResourceProfileCounter());
        if (requestSlotMatches.size() == pendingRequests.size()) {
            reserveMatchedFreeSlots(requestSlotMatches);
            fulfillMatchedSlots(requestSlotMatches);
            receivedSlots.clear();
        } else if (requestSlotMatches.size() < pendingRequests.size()) {
            // do nothing.
        } else {
            // requestSlotMatches.size() > pendingRequests.size()
            throw new IllegalStateException(
                    "The number of matched slots is not equals to the pendingRequests.");
        }
    }

    @VisibleForTesting
    Set<PhysicalSlot> getReceivedSlots() {
        return receivedSlots;
    }

    private void fulfillMatchedSlots(
            Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches) {
        // we have to first reserve all matching slots before fulfilling the requests
        // otherwise it can happen that the scheduler reserves one of the new slots
        // for a request which has been triggered by fulfilling a pending request
        for (RequestSlotMatchingStrategy.RequestSlotMatch requestSlotMatch : requestSlotMatches) {
            final PendingRequest pendingRequest = requestSlotMatch.getPendingRequest();
            final PhysicalSlot slot = requestSlotMatch.getSlot();

            Preconditions.checkState(
                    pendingRequest.fulfill(slot), "Pending requests must be fulfillable.");
        }
    }

    private void reserveMatchedFreeSlots(
            Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches) {
        for (RequestSlotMatchingStrategy.RequestSlotMatch match : requestSlotMatches) {
            final PendingRequest pendingRequest = match.getPendingRequest();
            final PhysicalSlot slot = match.getSlot();

            log.debug("Matched pending request {} with slot {}.", pendingRequest, slot);

            Preconditions.checkNotNull(
                    pendingRequests.remove(pendingRequest.getSlotRequestId()),
                    "Cannot fulfill a non existing pending slot request.");

            log.debug(
                    "Reserve slot {} for slot request id {}",
                    slot.getAllocationId(),
                    pendingRequest.getSlotRequestId());
            // 处理新匹配的 slot.
            getDeclarativeSlotPool()
                    .reserveFreeSlot(
                            slot.getAllocationId(), pendingRequest.getLoadableResourceProfile());
            fulfilledRequests.put(pendingRequest.getSlotRequestId(), slot.getAllocationId());
        }
    }

    @Override
    public Optional<PhysicalSlot> allocateAvailableSlot(
            @Nonnull SlotRequestId slotRequestId,
            @Nonnull AllocationID allocationID,
            @Nonnull LoadableResourceProfile requirementProfile) {
        assertRunningInMainThread();
        Preconditions.checkNotNull(requirementProfile, "The requiredSlotProfile must not be null.");

        log.debug(
                "Reserving free slot {} for slot request id {} and profile {}.",
                allocationID,
                slotRequestId,
                requirementProfile);

        return Optional.of(
                reserveFreeSlotForResource(slotRequestId, allocationID, requirementProfile));
    }

    // 申请空闲的 slot,
    private PhysicalSlot reserveFreeSlotForResource(
            SlotRequestId slotRequestId,
            AllocationID allocationId,
            LoadableResourceProfile requiredSlotProfile) {
        getDeclarativeSlotPool()
                .increaseResourceRequirementsBy(ResourceCounter.withResource(requiredSlotProfile));
        final PhysicalSlot physicalSlot =
                getDeclarativeSlotPool().reserveFreeSlot(allocationId, requiredSlotProfile);
        fulfilledRequests.put(slotRequestId, allocationId);

        return physicalSlot;
    }

    @VisibleForTesting
    @Nonnull
    public CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
            @Nonnull SlotRequestId slotRequestId,
            @Nonnull ResourceProfile resourceProfile,
            @Nonnull Collection<AllocationID> preferredAllocations,
            @Nullable Time timeout) {
        return requestNewAllocatedSlot(
                slotRequestId,
                resourceProfile.toEmptyLoadsResourceProfile(),
                preferredAllocations,
                timeout);
    }

    @Override
    @Nonnull
    public CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
            @Nonnull SlotRequestId slotRequestId,
            @Nonnull LoadableResourceProfile resourceProfile,
            @Nonnull Collection<AllocationID> preferredAllocations,
            @Nullable Time timeout) {
        assertRunningInMainThread();

        log.debug(
                "Request new allocated slot with slot request id {} and resource profile {}",
                slotRequestId,
                resourceProfile);

        final PendingRequest pendingRequest =
                PendingRequest.createNormalRequest(
                        slotRequestId, resourceProfile, preferredAllocations);

        return internalRequestNewSlot(pendingRequest, timeout);
    }

    @Override
    @Nonnull
    public CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(
            @Nonnull SlotRequestId slotRequestId,
            @Nonnull LoadableResourceProfile resourceProfile,
            @Nonnull Collection<AllocationID> preferredAllocations) {
        assertRunningInMainThread();

        log.debug(
                "Request new allocated batch slot with slot request id {} and resource profile {}",
                slotRequestId,
                resourceProfile);

        final PendingRequest pendingRequest =
                PendingRequest.createBatchRequest(
                        slotRequestId, resourceProfile, preferredAllocations);

        return internalRequestNewSlot(pendingRequest, null);
    }

    private CompletableFuture<PhysicalSlot> internalRequestNewSlot(
            PendingRequest pendingRequest, @Nullable Time timeout) {
        internalRequestNewAllocatedSlot(pendingRequest);

        if (timeout == null) {
            return pendingRequest.getSlotFuture();
        } else {
            return FutureUtils.orTimeout(
                            pendingRequest.getSlotFuture(),
                            timeout.toMilliseconds(),
                            TimeUnit.MILLISECONDS,
                            componentMainThreadExecutor,
                            String.format(
                                    "Pending slot request %s timed out after %d ms.",
                                    pendingRequest.getSlotRequestId(), timeout.toMilliseconds()))
                    .whenComplete(
                            (physicalSlot, throwable) -> {
                                if (throwable instanceof TimeoutException) {
                                    timeoutPendingSlotRequest(pendingRequest.getSlotRequestId());
                                }
                            });
        }
    }

    private void timeoutPendingSlotRequest(SlotRequestId slotRequestId) {
        releaseSlot(
                slotRequestId,
                new TimeoutException("Pending slot request timed out in slot pool."));
    }

    private void internalRequestNewAllocatedSlot(PendingRequest pendingRequest) {
        pendingRequests.put(pendingRequest.getSlotRequestId(), pendingRequest);

        getDeclarativeSlotPool()
                .increaseResourceRequirementsBy(
                        ResourceCounter.withResource(pendingRequest.getLoadableResourceProfile()));
    }

    @Override
    protected void onFailAllocation(ResourceCounter previouslyFulfilledRequirements) {
        getDeclarativeSlotPool().decreaseResourceRequirementsBy(previouslyFulfilledRequirements);
    }

    @Override
    public void releaseSlot(@Nonnull SlotRequestId slotRequestId, @Nullable Throwable cause) {
        log.debug("Release slot with slot request id {}", slotRequestId);
        assertRunningInMainThread();

        final PendingRequest pendingRequest = pendingRequests.remove(slotRequestId);

        if (pendingRequest != null) {
            getDeclarativeSlotPool()
                    .decreaseResourceRequirementsBy(
                            ResourceCounter.withResource(
                                    pendingRequest.getLoadableResourceProfile()));
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
    public void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {
        assertRunningInMainThread();

        failPendingRequests(acquiredResources);
    }

    private void failPendingRequests(Collection<ResourceRequirement> acquiredResources) {
        // only fails streaming requests because batch jobs do not require all resources
        // requirements to be fullfilled at the same time
        Predicate<PendingRequest> predicate = request -> !request.isBatchRequest();
        if (pendingRequests.values().stream().anyMatch(predicate)) {
            log.warn(
                    "Could not acquire the minimum required resources, failing slot requests. Acquired: {}. Current slot pool status: {}",
                    acquiredResources,
                    getSlotServiceStatus());
            cancelPendingRequests(
                    predicate,
                    NoResourceAvailableException.withoutStackTrace(
                            "Could not acquire the minimum required resources."));
        }
    }

    @Override
    public Collection<SlotInfo> getAllocatedSlotsInformation() {
        assertRunningInMainThread();

        final Collection<? extends SlotInfo> allSlotsInformation =
                getDeclarativeSlotPool().getAllSlotsInformation();
        final Set<AllocationID> freeSlots =
                getDeclarativeSlotPool().getFreeSlotInfoTracker().getAvailableSlots();

        return allSlotsInformation.stream()
                .filter(slotInfo -> !freeSlots.contains(slotInfo.getAllocationId()))
                .collect(Collectors.toList());
    }

    @Override
    public FreeSlotInfoTracker getFreeSlotInfoTracker() {
        assertRunningInMainThread();

        return getDeclarativeSlotPool().getFreeSlotInfoTracker();
    }

    @Override
    public void disableBatchSlotRequestTimeoutCheck() {
        isBatchSlotRequestTimeoutCheckDisabled = true;
    }

    private void assertRunningInMainThread() {
        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.assertRunningInMainThread();
        } else {
            throw new IllegalStateException("The FutureSlotPool has not been started yet.");
        }
    }

    private void checkIdleSlotTimeout() {
        getDeclarativeSlotPool().releaseIdleSlots(getRelativeTimeMillis());

        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.schedule(
                    this::checkIdleSlotTimeout,
                    idleSlotTimeout.toMilliseconds(),
                    TimeUnit.MILLISECONDS);
        }
    }

    void checkBatchSlotTimeout() {
        assertRunningInMainThread();

        if (isBatchSlotRequestTimeoutCheckDisabled) {
            return;
        }

        final Collection<PendingRequest> pendingBatchRequests = getPendingBatchRequests();

        if (!pendingBatchRequests.isEmpty()) {
            final Set<LoadableResourceProfile> allResourceProfiles =
                    getResourceProfilesFromAllSlots();

            final Map<Boolean, List<PendingRequest>> fulfillableAndUnfulfillableRequests =
                    pendingBatchRequests.stream()
                            .collect(
                                    Collectors.partitioningBy(
                                            canBeFulfilledWithAnySlot(allResourceProfiles)));

            final List<PendingRequest> fulfillableRequests =
                    fulfillableAndUnfulfillableRequests.get(true);
            final List<PendingRequest> unfulfillableRequests =
                    fulfillableAndUnfulfillableRequests.get(false);

            final long currentTimestamp = getRelativeTimeMillis();

            for (PendingRequest fulfillableRequest : fulfillableRequests) {
                fulfillableRequest.markFulfillable();
            }

            for (PendingRequest unfulfillableRequest : unfulfillableRequests) {
                unfulfillableRequest.markUnfulfillable(currentTimestamp);

                if (unfulfillableRequest.getUnfulfillableSince() + batchSlotTimeout.toMilliseconds()
                        <= currentTimestamp) {
                    timeoutPendingSlotRequest(unfulfillableRequest.getSlotRequestId());
                }
            }
        }

        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.schedule(
                    this::checkBatchSlotTimeout,
                    batchSlotTimeout.toMilliseconds(),
                    TimeUnit.MILLISECONDS);
        }
    }

    private Set<LoadableResourceProfile> getResourceProfilesFromAllSlots() {
        return Stream.concat(
                        getFreeSlotInfoTracker().getFreeSlotsInformation().stream(),
                        getAllocatedSlotsInformation().stream())
                .map(SlotInfo::getLoadableResourceProfile)
                .collect(Collectors.toSet());
    }

    private Collection<PendingRequest> getPendingBatchRequests() {
        return pendingRequests.values().stream()
                .filter(PendingRequest::isBatchRequest)
                .collect(Collectors.toList());
    }

    private static Predicate<PendingRequest> canBeFulfilledWithAnySlot(
            Set<LoadableResourceProfile> allocatedResourceProfiles) {
        return pendingRequest -> {
            for (LoadableResourceProfile allocatedResourceProfile : allocatedResourceProfiles) {
                if (allocatedResourceProfile.isMatching(
                        pendingRequest.getLoadableResourceProfile())) {
                    return true;
                }
            }

            return false;
        };
    }

    @VisibleForTesting
    public int getNumPendingRequests() {
        return pendingRequests.size();
    }

    @VisibleForTesting
    void increaseResourceRequirementsBy(ResourceCounter increment) {
        getDeclarativeSlotPool().increaseResourceRequirementsBy(increment);
    }

    @VisibleForTesting
    boolean isBatchSlotRequestTimeoutCheckEnabled() {
        return !isBatchSlotRequestTimeoutCheckDisabled;
    }

    @VisibleForTesting
    public boolean isSlotBatchAllocatable() {
        return slotBatchAllocatable;
    }
}
