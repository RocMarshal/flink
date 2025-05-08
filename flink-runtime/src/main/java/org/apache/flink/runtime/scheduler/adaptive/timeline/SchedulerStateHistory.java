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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utils class to record scheduler state change list. */
public class SchedulerStateHistory {

    private static final int HISTOGRAM_WINDOW_SIZE = 64;

    private final List<SchedulerStateSpan> schedulerStatesHistorySpan;

    private final Map<String, StatsSummary> stats;

    public SchedulerStateHistory() {
        this.schedulerStatesHistorySpan = new ArrayList<>();
        this.stats = new HashMap<>();
    }

    public void addSchedulerStateSpan(SchedulerStateSpan schedulerStateSpan) {
        schedulerStatesHistorySpan.add(schedulerStateSpan);
        if (schedulerStateSpan.isSealed() && schedulerStateSpan.getDuration().toMillis() > 0L) {
            stats.compute(
                    schedulerStateSpan.getState(),
                    (s, statsSummary) -> {
                        StatsSummary summary =
                                statsSummary != null
                                        ? statsSummary
                                        : new StatsSummary(HISTOGRAM_WINDOW_SIZE);
                        summary.add(schedulerStateSpan.getDuration().toMillis());
                        return summary;
                    });
        }
    }

    public List<SchedulerStateSpan> getSchedulerStatesHistory() {
        return Collections.unmodifiableList(schedulerStatesHistorySpan);
    }
}
