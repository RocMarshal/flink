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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolBridgeTest.createAllocatedSlot;
import static org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolBridgeTest.createDeclarativeSlotPoolBridge;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DeclarativeSlotPoolBridge}. */
@ExtendWith(ParameterizedTestExtension.class)
class DeclarativeSlotPoolBridgeResourceDeclarationTest extends DeclarativeSlotPoolBridgeTestBase {

    private RequirementListener requirementListener;
    private DeclarativeSlotPoolBridge declarativeSlotPoolBridge;

    @BeforeEach
    void setup() {
        requirementListener = new RequirementListener(mainThreadExecutor, slotRequestMaxInterval);

        final TestingDeclarativeSlotPoolBuilder slotPoolBuilder =
                TestingDeclarativeSlotPool.builder()
                        .setIncreaseResourceRequirementsByConsumer(
                                requirementListener::increaseRequirements)
                        .setDecreaseResourceRequirementsByConsumer(
                                requirementListener::decreaseRequirements)
                        .setReserveFreeSlotFunction(
                                (allocationId, resourceProfile) ->
                                        createAllocatedSlot(allocationId))
                        .setFreeReservedSlotFunction(
                                (allocationID, throwable, aLong) ->
                                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1))
                        .setReleaseSlotFunction(
                                (allocationID, e) ->
                                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1));

        final TestingDeclarativeSlotPoolFactory declarativeSlotPoolFactory =
                new TestingDeclarativeSlotPoolFactory(slotPoolBuilder);
        declarativeSlotPoolBridge =
                createDeclarativeSlotPoolBridge(
                        declarativeSlotPoolFactory,
                        requestSlotMatchingStrategy,
                        requirementListener,
                        slotBatchAllocatable);
    }

    @AfterEach
    void teardown() {
        if (declarativeSlotPoolBridge != null) {
            declarativeSlotPoolBridge.close();
        }
    }

    @TestTemplate
    void testRequirementsIncreasedOnNewAllocation() throws Exception {
        declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost", mainThreadExecutor);

        // requesting the allocation of a new slot should increase the requirements
        declarativeSlotPoolBridge.requestNewAllocatedSlot(
                new SlotRequestId(), ResourceProfile.UNKNOWN.toEmptyLoadable(), Time.minutes(5));

        declarativeSlotPoolBridge.tryWaitSlotRequestMaxIntervalTimeout();

        assertThat(requirementListener.getRequirements().getResourceCount(ResourceProfile.UNKNOWN))
                .isOne();
    }

    @TestTemplate
    void testRequirementsDecreasedOnAllocationTimeout() throws Exception {
        final ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor();
        try {
            ComponentMainThreadExecutor mainThreadExecutor =
                    ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                            scheduledExecutorService);
            declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost", mainThreadExecutor);

            // requesting the allocation of a new slot increases the requirements
            final CompletableFuture<PhysicalSlot> allocationFuture =
                    CompletableFuture.supplyAsync(
                                    () ->
                                            declarativeSlotPoolBridge.requestNewAllocatedSlot(
                                                    new SlotRequestId(),
                                                    ResourceProfile.UNKNOWN.toEmptyLoadable(),
                                                    Time.milliseconds(50)),
                                    mainThreadExecutor)
                            .get();

            declarativeSlotPoolBridge.tryWaitSlotRequestMaxIntervalTimeout();

            // waiting for the timeout
            assertThatFuture(allocationFuture).failsWithin(Duration.ofMinutes(1));
            // when the allocation fails the requirements should be reduced (it is the users
            // responsibility to retry)
            CompletableFuture.runAsync(
                            () ->
                                    assertThat(
                                                    requirementListener
                                                            .getRequirements()
                                                            .getResourceCount(
                                                                    ResourceProfile.UNKNOWN))
                                            .isZero(),
                            mainThreadExecutor)
                    .join();
        } finally {
            scheduledExecutorService.shutdown();
        }
    }

    @TestTemplate
    void testRequirementsUnchangedOnNewSlotsNotification() throws Exception {
        declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost", mainThreadExecutor);

        // notifications about new slots should not affect requirements
        final PhysicalSlot newSlot = createAllocatedSlot(new AllocationID());
        declarativeSlotPoolBridge.newSlotsAreAvailable(Collections.singleton(newSlot));
        assertThat(requirementListener.getRequirements().getResourceCount(ResourceProfile.UNKNOWN))
                .isZero();
    }

    @TestTemplate
    void testRequirementsIncreasedOnSlotReservation() throws Exception {
        declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost", mainThreadExecutor);

        final PhysicalSlot newSlot = createAllocatedSlot(new AllocationID());
        declarativeSlotPoolBridge.newSlotsAreAvailable(Collections.singleton(newSlot));

        // allocating (==reserving) an available (==free) slot should increase the requirements
        final SlotRequestId slotRequestId = new SlotRequestId();
        declarativeSlotPoolBridge.allocateAvailableSlot(
                slotRequestId,
                newSlot.getAllocationId(),
                ResourceProfile.UNKNOWN.toEmptyLoadable());

        declarativeSlotPoolBridge.tryWaitSlotRequestMaxIntervalTimeout();

        assertThat(requirementListener.getRequirements().getResourceCount(ResourceProfile.UNKNOWN))
                .isOne();
    }

    @TestTemplate
    void testRequirementsDecreasedOnSlotFreeing() throws Exception {
        declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost", mainThreadExecutor);

        final PhysicalSlot newSlot = createAllocatedSlot(new AllocationID());
        declarativeSlotPoolBridge.newSlotsAreAvailable(Collections.singleton(newSlot));

        final SlotRequestId slotRequestId = new SlotRequestId();
        declarativeSlotPoolBridge.allocateAvailableSlot(
                slotRequestId,
                newSlot.getAllocationId(),
                ResourceProfile.UNKNOWN.toEmptyLoadable());

        declarativeSlotPoolBridge.tryWaitSlotRequestMaxIntervalTimeout();

        // releasing (==freeing) a [reserved] slot should decrease the requirements
        declarativeSlotPoolBridge.releaseSlot(
                slotRequestId, new RuntimeException("Test exception"));
        assertThat(requirementListener.getRequirements().getResourceCount(ResourceProfile.UNKNOWN))
                .isZero();
    }

    @TestTemplate
    void testRequirementsDecreasedOnSlotAllocationFailure() throws Exception {
        declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost", mainThreadExecutor);

        final PhysicalSlot newSlot = createAllocatedSlot(new AllocationID());
        declarativeSlotPoolBridge.newSlotsAreAvailable(Collections.singleton(newSlot));

        declarativeSlotPoolBridge.allocateAvailableSlot(
                new SlotRequestId(),
                newSlot.getAllocationId(),
                ResourceProfile.UNKNOWN.toEmptyLoadable());

        declarativeSlotPoolBridge.tryWaitSlotRequestMaxIntervalTimeout();

        // releasing (==freeing) a [reserved] slot should decrease the requirements
        declarativeSlotPoolBridge.failAllocation(
                newSlot.getTaskManagerLocation().getResourceID(),
                newSlot.getAllocationId(),
                new RuntimeException("Test exception"));
        assertThat(requirementListener.getRequirements().getResourceCount(ResourceProfile.UNKNOWN))
                .isZero();
    }

    /** Requirement listener for testing. */
    public static final class RequirementListener {

        ComponentMainThreadExecutor componentMainThreadExecutor;
        Duration slotRequestMaxInterval;
        ScheduledFuture<?> slotRequestMaxIntervalTimeoutFuture;

        RequirementListener() {}

        RequirementListener(
                ComponentMainThreadExecutor componentMainThreadExecutor,
                @Nonnull Duration slotRequestMaxInterval) {
            this.componentMainThreadExecutor = componentMainThreadExecutor;
            this.slotRequestMaxInterval = slotRequestMaxInterval;
        }

        private ResourceCounter requirements = ResourceCounter.empty();

        private void increaseRequirements(ResourceCounter requirements) {
            if (slotRequestMaxInterval.toMillis() <= 0L) {
                this.requirements = this.requirements.add(requirements);
                return;
            }

            if (!slotRequestMaxIntervalTimeoutFutureAssignable()) {
                slotRequestMaxIntervalTimeoutFuture.cancel(true);
            }
            slotRequestMaxIntervalTimeoutFuture =
                    componentMainThreadExecutor.schedule(
                            () -> this.checkSlotRequestMaxIntervalTimeout(requirements),
                            slotRequestMaxInterval.toMillis(),
                            TimeUnit.MILLISECONDS);
        }

        private void decreaseRequirements(ResourceCounter requirements) {
            this.requirements = this.requirements.subtract(requirements);
        }

        public ResourceCounter getRequirements() {
            return requirements;
        }

        public void tryWaitSlotRequestIntervalTimeout() {
            if (Objects.nonNull(slotRequestMaxIntervalTimeoutFuture)) {
                try {
                    slotRequestMaxIntervalTimeoutFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private boolean slotRequestMaxIntervalTimeoutFutureAssignable() {
            return slotRequestMaxIntervalTimeoutFuture == null
                    || slotRequestMaxIntervalTimeoutFuture.isDone()
                    || slotRequestMaxIntervalTimeoutFuture.isCancelled();
        }

        private void checkSlotRequestMaxIntervalTimeout(ResourceCounter requirements) {
            if (slotRequestMaxInterval.toMillis() <= 0L) {
                return;
            }
            this.requirements = this.requirements.add(requirements);
        }
    }
}
