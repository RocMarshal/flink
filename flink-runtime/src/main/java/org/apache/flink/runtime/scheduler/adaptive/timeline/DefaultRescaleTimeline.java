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

import org.apache.flink.runtime.scheduler.adaptive.JobGraphJobInformation;
import org.apache.flink.runtime.util.BoundedFIFOQueue;
import org.apache.flink.util.AbstractID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Default implementation of {@link RescaleTimeline}. */
public class DefaultRescaleTimeline implements RescaleTimeline {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRescaleTimeline.class);

    private final Supplier<JobGraphJobInformation> jobInfoGetter;

    private IdEpoch idEpoch;

    @Nullable private Rescale currentRescale;

    private final BoundedFIFOQueue<Rescale> rescaleHistory;

    private final Map<RescaleStatus, Rescale> latestRescales;

    private final LinkedHashMap<Long, Rescale> recentRescales;

    private final RescalesSummary rescalesSummary;

    public DefaultRescaleTimeline(
            int maxHistorySize, Supplier<JobGraphJobInformation> jobInfoGetter) {
        this.idEpoch = new IdEpoch(0, new AbstractID(), 0L);
        this.jobInfoGetter = jobInfoGetter;
        this.latestRescales = new ConcurrentHashMap<>(RescaleStatus.values().length);
        this.rescaleHistory = new BoundedFIFOQueue<>(maxHistorySize);
        this.recentRescales =
                new LinkedHashMap<>() {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry eldest) {
                        return size() > maxHistorySize;
                    }
                };
        this.rescalesSummary = new RescalesSummary(maxHistorySize);
    }

    @Nullable
    @Override
    public Rescale latestRescale(RescaleStatus status) {
        return latestRescales.get(status);
    }

    @Override
    public boolean inIdling() {
        return currentRescale == null || currentRescale.isSealed();
    }

    @Override
    public boolean inPending() {
        return !inIdling();
    }

    @Nullable
    @Override
    public Rescale currentRescale() {
        return currentRescale;
    }

    private IdEpoch nextRescaleId(boolean newRescaleEpoch) {
        if (newRescaleEpoch) {
            idEpoch = new IdEpoch(idEpoch.getRescaleId() + 1L, new AbstractID(), 1L);
        } else {
            idEpoch =
                    new IdEpoch(
                            idEpoch.getRescaleId() + 1,
                            idEpoch.getResourceRequirementsEpoch(),
                            idEpoch.getSubRescaleIdOfCurrentEpoch() + 1L);
        }
        return idEpoch;
    }

    /** Rolling the last rescale for the specified status. */
    private void rollingLatestRescale() {
        if (Rescale.isSealed(currentRescale)) {
            latestRescales.put(currentRescale.getStatus(), currentRescale);
        }
    }

    @Override
    public boolean newCurrentRescale(boolean newRescaleEpoch) {
        rollingLatestRescale();
        if (inIdling()) {
            currentRescale = new Rescale(nextRescaleId(newRescaleEpoch), newRescaleEpoch);
            rescaleHistory.add(currentRescale);
            recentRescales.put(currentRescale.getIdEpoch().getRescaleId(), currentRescale);
            rescalesSummary.addPending(currentRescale);
            return true;
        }
        LOG.warn("Rescale {} with unexpected status", currentRescale);
        return false;
    }

    @Override
    public boolean updateCurrentRescale(Consumer<Rescale> updateAction) {
        if (inPending() && Objects.nonNull(updateAction)) {
            updateAction.accept(currentRescale);
            rollingLatestRescale();
            if (Rescale.isSealed(currentRescale)) {
                rescalesSummary.addSealed(currentRescale);
            }
            return true;
        }
        return false;
    }

    @Override
    public JobGraphJobInformation getJobInformation() {
        return jobInfoGetter.get();
    }
}
