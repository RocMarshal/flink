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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RescalesStatsSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<Rescale> rescaleHistory;
    private final Map<RescaleStatus, Rescale> latestRescales;
    private final Map<Long, Rescale> recentRescales;
    private final RescalesSummarySnapshot rescalesSummarySnapshot;

    public RescalesStatsSnapshot(
            List<Rescale> rescaleHistory,
            Map<RescaleStatus, Rescale> latestRescales,
            Map<Long, Rescale> recentRescales,
            RescalesSummarySnapshot rescalesSummarySnapshot) {
        this.rescaleHistory = rescaleHistory;
        this.latestRescales = latestRescales;
        this.recentRescales = recentRescales;
        this.rescalesSummarySnapshot = rescalesSummarySnapshot;
    }

    public List<Rescale> getRescaleHistory() {
        return rescaleHistory;
    }

    public Map<RescaleStatus, Rescale> getLatestRescales() {
        return latestRescales;
    }

    public Map<Long, Rescale> getRecentRescales() {
        return recentRescales;
    }

    public RescalesSummarySnapshot getRescalesSummarySnapshot() {
        return rescalesSummarySnapshot;
    }

    public static RescalesStatsSnapshot emptySnapshot() {
        return new RescalesStatsSnapshot(
                new ArrayList<>(),
                new HashMap<>(),
                new HashMap<>(),
                new RescalesSummarySnapshot(
                        StatsSummarySnapshot.empty(),
                        StatsSummarySnapshot.empty(),
                        StatsSummarySnapshot.empty(),
                        StatsSummarySnapshot.empty(),
                        0,
                        0,
                        0,
                        0,
                        0));
    }
}
