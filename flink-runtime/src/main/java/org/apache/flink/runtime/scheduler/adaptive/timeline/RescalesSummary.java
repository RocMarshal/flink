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

import org.apache.flink.runtime.util.stats.StatsSummary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Statistics summary of rescales. */
public class RescalesSummary {

    private static final Logger LOG = LoggerFactory.getLogger(RescalesSummary.class);

    private final StatsSummary allSealedSummary;

    private final StatsSummary completedRescalesSummary;
    private final StatsSummary ignoredRescalesSummary;
    private final StatsSummary failedRescalesSummary;

    private long totalRescalesCount = 0L;
    private long completedRescalesCount = 0L;
    private long ignoredRescalesCount = 0L;
    private long failedRescalesCount = 0L;
    private long inProgressRescalesCount = 0L;

    public RescalesSummary(int maxHistorySize) {
        this.allSealedSummary = new StatsSummary(maxHistorySize);
        this.completedRescalesSummary = new StatsSummary(maxHistorySize);
        this.ignoredRescalesSummary = new StatsSummary(maxHistorySize);
        this.failedRescalesSummary = new StatsSummary(maxHistorySize);
    }

    public void addTerminated(Rescale rescale) {
        if (!Rescale.isTerminated(rescale)) {
            LOG.warn(
                    "Unexpected rescale: {}, which will be ignored when computing statistics.",
                    rescale);
            return;
        }

        allSealedSummary.add(rescale.getDuration().toMillis());
        if (inProgressRescalesCount > 0) {
            inProgressRescalesCount--;
        }

        if (rescale.getTerminalState() == null) {
            return;
        }

        switch (rescale.getTerminalState()) {
            case FAILED:
                failedRescalesSummary.add(rescale.getDuration().toMillis());
                failedRescalesCount++;
                break;
            case COMPLETED:
                completedRescalesSummary.add(rescale.getDuration().toMillis());
                completedRescalesCount++;
                break;
            case IGNORED:
                ignoredRescalesSummary.add(rescale.getDuration().toMillis());
                ignoredRescalesCount++;
                break;
            default:
                break;
        }
    }

    public void addInProgress(Rescale rescale) {
        if (!Rescale.isTerminated(rescale)) {
            LOG.warn("Unexpected rescale: {}, which will be ignored.", rescale);
        } else {
            if (inProgressRescalesCount < 1) {
                inProgressRescalesCount++;
            }
            this.totalRescalesCount++;
        }
    }

    public long getTotalRescalesCount() {
        return totalRescalesCount;
    }

    public long getInProgressRescalesCount() {
        return inProgressRescalesCount;
    }

    public RescalesSummarySnapshot createSnapshot() {
        return new RescalesSummarySnapshot(
                allSealedSummary.createSnapshot(),
                completedRescalesSummary.createSnapshot(),
                ignoredRescalesSummary.createSnapshot(),
                failedRescalesSummary.createSnapshot(),
                totalRescalesCount,
                completedRescalesCount,
                ignoredRescalesCount,
                failedRescalesCount,
                pendingRescalesCount);
    }
}
