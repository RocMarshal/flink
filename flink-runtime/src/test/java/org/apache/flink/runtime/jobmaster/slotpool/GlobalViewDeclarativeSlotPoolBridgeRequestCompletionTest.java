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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.CheckedSupplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests how the {@link GlobalViewDeclarativeSlotPoolBridge} completes slot requests. */
@ExtendWith(TestLoggerExtension.class)
class GlobalViewDeclarativeSlotPoolBridgeRequestCompletionTest {

    private static final Time TIMEOUT = SlotPoolUtils.TIMEOUT;

    private static final Time SLOT_REQUEST_MAX_INTERVAL = Time.seconds(1L);

    private TestingResourceManagerGateway resourceManagerGateway;

    @BeforeEach
    void setUp() {
        resourceManagerGateway = new TestingResourceManagerGateway();
    }

    /**
     * Tests that the {@link GlobalViewDeclarativeSlotPoolBridge} completes slots after slot request
     * max interval.
     */
    @Test
    void testRequestsAreCompletedAfterSlotRequestMaxInterval() {
        try (final SlotPool slotPool =
                CheckedSupplier.unchecked(this::createAndSetUpSlotPool).get()) {

            final int requestNum = 10;

            final List<SlotRequestId> slotRequestIds =
                    IntStream.range(0, requestNum)
                            .mapToObj(ignored -> new SlotRequestId())
                            .collect(Collectors.toList());

            final List<CompletableFuture<PhysicalSlot>> slotRequests =
                    slotRequestIds.stream()
                            .map(
                                    slotRequestId ->
                                            slotPool.requestNewAllocatedSlot(
                                                    slotRequestId,
                                                    ResourceProfile.UNKNOWN,
                                                    TIMEOUT))
                            .collect(Collectors.toList());
            final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
            slotPool.registerTaskManager(taskManagerLocation.getResourceID());

            Thread.sleep(SLOT_REQUEST_MAX_INTERVAL.toMilliseconds());

            List<SlotOffer> slotOffers = generateSlotOffers(requestNum);

            final Collection<SlotOffer> acceptedSlots =
                    slotPool.offerSlots(
                            taskManagerLocation, new SimpleAckingTaskManagerGateway(), slotOffers);
            assertThat(acceptedSlots).containsAll(slotOffers);
            FutureUtils.waitForAll(slotRequests);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private List<SlotOffer> generateSlotOffers(int number) {
        Preconditions.checkArgument(number >= 0);
        List<SlotOffer> slotOffers = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            final SlotOffer slotOffer = new SlotOffer(new AllocationID(), i, ResourceProfile.ANY);
            slotOffers.add(slotOffer);
        }
        return slotOffers;
    }

    private SlotPool createAndSetUpSlotPool() throws Exception {
        return new DeclarativeSlotPoolBridgeBuilder()
                .setResourceManagerGateway(resourceManagerGateway)
                .buildGlobalViewDeclarativeSlotPoolBridgeAndStart(
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        SLOT_REQUEST_MAX_INTERVAL);
    }
}
