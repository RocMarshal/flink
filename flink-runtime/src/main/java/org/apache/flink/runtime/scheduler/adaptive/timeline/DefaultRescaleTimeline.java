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

package org.apache.flink.runtime.scheduler.adaptive.timeline;

import org.apache.flink.runtime.checkpoint.StatsSummary;
import org.apache.flink.runtime.scheduler.adaptive.JobGraphJobInformation;
import org.apache.flink.runtime.util.BoundedFIFOQueue;
import org.apache.flink.util.AbstractID;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Default implementation of {@link RescaleTimeline}. */
public class DefaultRescaleTimeline implements RescaleTimeline {

    private static final int HISTOGRAM_WINDOW_SIZE = 128;

    private final transient Logger log;

    private IdEpoch idEpoch;

    @Nullable private Rescale lastCompletedRescale;
    @Nullable private Rescale currentRescale;

    private final Supplier<JobGraphJobInformation> jobInfoGetter;
    private final BoundedFIFOQueue<Rescale> rescaleHistory;
    private final Map<RescaleStatus, StatsSummary> statsMap;

    public DefaultRescaleTimeline(
            int maxHistorySize, Supplier<JobGraphJobInformation> jobInfoGetter, Logger log) {
        this.idEpoch = new IdEpoch(0, new AbstractID(), 0);
        this.jobInfoGetter = jobInfoGetter;
        // The history is not empty forever.
        this.rescaleHistory = new BoundedFIFOQueue<>(maxHistorySize);
        this.statsMap = new HashMap<>();
        this.log = log;
    }

    @Nullable
    public Rescale lastCompletedRescale() {
        return lastCompletedRescale;
    }

    public List<Rescale> historyToList() {
        return rescaleHistory.toArrayList();
    }

    @Override
    public IdEpoch nextRescaleId(boolean newRescaleEpoch) {
        if (newRescaleEpoch) {
            idEpoch = new IdEpoch(idEpoch.getRescaleId() + 1L, new AbstractID(), 1);
        } else {
            idEpoch =
                    new IdEpoch(
                            idEpoch.getRescaleId() + 1,
                            idEpoch.getResourceRequirementsEpoch(),
                            idEpoch.getSubRescaleIdOfCurrentEpoch() + 1);
        }
        return idEpoch;
    }

    @Override
    public void tryRolloutCurrentPendingRescale(
            boolean newRescaleEpoch,
            @Nullable Consumer<Rescale> sealAction,
            @Nullable Consumer<Rescale> updateAction) {
        if (isCurrentRescaleExistedNonSealed() && Objects.nonNull(sealAction)) {
            sealAction.accept(currentRescale);
            statsMap.compute(
                    currentRescale.getStatus(),
                    (rescaleStatus, oldVal) -> {
                        StatsSummary newValue =
                                oldVal == null ? new StatsSummary(HISTOGRAM_WINDOW_SIZE) : oldVal;
                        newValue.add(currentRescale.getDuration().toMillis());
                        return newValue;
                    });
        }
        rollingLastCompleted();
        if (isCurrentRescaleSwitchable() && Objects.nonNull(updateAction)) {
            currentRescale = new Rescale(this, log, newRescaleEpoch);
        }
        if (Objects.nonNull(updateAction) && isCurrentRescaleExistedNonSealed()) {
            updateAction.accept(currentRescale);
        } else {
            log.debug("RescaleTimeline does not have any updates to be performed.");
        }
    }

    @Override
    public void tryUpdateCurrentPendingRescale(Consumer<Rescale> updateAction) {
        if (isCurrentRescaleExistedNonSealed() && Objects.nonNull(updateAction)) {
            updateAction.accept(currentRescale);
        }
    }

    private boolean isCurrentRescaleSwitchable() {
        return currentRescale == null || currentRescale.isSealed();
    }

    private boolean isCurrentRescaleExistedNonSealed() {
        return currentRescale != null && !currentRescale.isSealed();
    }

    private void rollingLastCompleted() {
        if (currentRescale != null && currentRescale.getStatus() == RescaleStatus.Completed) {
            this.lastCompletedRescale = currentRescale;
        }
    }

    @Override
    public JobGraphJobInformation getJobInformation() {
        return jobInfoGetter.get();
    }
}
