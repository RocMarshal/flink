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

import org.apache.flink.runtime.util.stats.StatsSummarySnapshot;

import java.io.Serializable;

public class RescalesSummarySnapshot implements Serializable {

    private final StatsSummarySnapshot allSealedSummarySnapshot;

    private final StatsSummarySnapshot completedRescalesSummarySnapshot;
    private final StatsSummarySnapshot ignoredRescalesSummarySnapshot;
    private final StatsSummarySnapshot failedRescalesSummarySnapshot;

    private long totalRescalesCount = 0L;
    private long completedRescalesCount = 0L;
    private long ignoredRescalesCount = 0L;
    private long failedRescalesCount = 0L;
    private long pendingRescalesCount = 0L;

    public RescalesSummarySnapshot(
            StatsSummarySnapshot allSealedSummarySnapshot,
            StatsSummarySnapshot completedRescalesSummarySnapshot,
            StatsSummarySnapshot ignoredRescalesSummarySnapshot,
            StatsSummarySnapshot failedRescalesSummarySnapshot,
            long totalRescalesCount,
            long completedRescalesCount,
            long ignoredRescalesCount,
            long failedRescalesCount,
            long pendingRescalesCount) {
        this.allSealedSummarySnapshot = allSealedSummarySnapshot;
        this.completedRescalesSummarySnapshot = completedRescalesSummarySnapshot;
        this.ignoredRescalesSummarySnapshot = ignoredRescalesSummarySnapshot;
        this.failedRescalesSummarySnapshot = failedRescalesSummarySnapshot;
        this.totalRescalesCount = totalRescalesCount;
        this.completedRescalesCount = completedRescalesCount;
        this.ignoredRescalesCount = ignoredRescalesCount;
        this.failedRescalesCount = failedRescalesCount;
        this.pendingRescalesCount = pendingRescalesCount;
    }

    public StatsSummarySnapshot getAllSealedSummarySnapshot() {
        return allSealedSummarySnapshot;
    }

    public StatsSummarySnapshot getCompletedRescalesSummarySnapshot() {
        return completedRescalesSummarySnapshot;
    }

    public StatsSummarySnapshot getIgnoredRescalesSummarySnapshot() {
        return ignoredRescalesSummarySnapshot;
    }

    public StatsSummarySnapshot getFailedRescalesSummarySnapshot() {
        return failedRescalesSummarySnapshot;
    }

    public long getTotalRescalesCount() {
        return totalRescalesCount;
    }

    public long getCompletedRescalesCount() {
        return completedRescalesCount;
    }

    public long getIgnoredRescalesCount() {
        return ignoredRescalesCount;
    }

    public long getFailedRescalesCount() {
        return failedRescalesCount;
    }

    public long getPendingRescalesCount() {
        return pendingRescalesCount;
    }
}
