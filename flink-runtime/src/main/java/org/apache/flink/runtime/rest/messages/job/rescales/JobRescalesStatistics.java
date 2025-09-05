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

package org.apache.flink.runtime.rest.messages.job.rescales;

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.util.stats.StatsSummaryDto;
import org.apache.flink.runtime.scheduler.adaptive.timeline.Rescale;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesStatsSnapshot;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesSummarySnapshot;
import org.apache.flink.runtime.scheduler.adaptive.timeline.TerminalState;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Schema(name = "JobRescalesStatistics")
public class JobRescalesStatistics implements ResponseBody, Serializable {

    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_SUMMARY = "summary";
    public static final String FIELD_NAME_LATEST = "latest";
    public static final String FIELD_NAME_HISTORY = "history";

    @JsonProperty(FIELD_NAME_SUMMARY)
    private final RescalesSummary summary;

    @JsonProperty(FIELD_NAME_LATEST)
    private final LatestRescales latest;

    @JsonProperty(FIELD_NAME_HISTORY)
    private final List<JobRescaleStatisticsDetails> history;

    @JsonCreator
    public JobRescalesStatistics(
            @JsonProperty(FIELD_NAME_SUMMARY) RescalesSummary summary,
            @JsonProperty(FIELD_NAME_LATEST) LatestRescales latest,
            @JsonProperty(FIELD_NAME_HISTORY) List<JobRescaleStatisticsDetails> history) {
        this.summary = summary;
        this.latest = latest;
        this.history = history;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobRescalesStatistics that = (JobRescalesStatistics) o;
        return Objects.equals(summary, that.summary)
                && Objects.equals(latest, that.latest)
                && Objects.equals(history, that.history);
    }

    @Override
    public int hashCode() {
        return Objects.hash(summary, latest, history);
    }

    public static JobRescalesStatistics fromRescalesStatsSnapshot(RescalesStatsSnapshot snapshot) {
        RescalesSummary rescalesSummary =
                RescalesSummary.fromRescalesSummarySnapshot(snapshot.getRescalesSummarySnapshot());
        LatestRescales latestRescales = LatestRescales.fromRescalesStatsSnapshot(snapshot);

        return new JobRescalesStatistics(
                rescalesSummary,
                latestRescales,
                snapshot.getRescaleHistory().stream()
                        .map(
                                (Rescale rescale) ->
                                        JobRescaleStatisticsDetails.fromRescale(rescale, false))
                        .collect(Collectors.toList()));
    }

    public static class RescalesSummary implements Serializable {

        private static final long serialVersionUID = 1L;

        public static final String FIELD_NAME_COUNTS = "rescales_counts";
        public static final String FIELD_NAME_RESCALE_DURATION = "rescales_duration_stats";
        public static final String FIELD_NAME_RESCALE_COMPLETED_DURATION =
                "completed_rescales_duration_stats";
        public static final String FIELD_NAME_RESCALE_IGNORED_DURATION =
                "ignored_rescales_duration_stats";
        public static final String FIELD_NAME_RESCALE_FAILED_DURATION =
                "failed_rescales_duration_stats";

        @JsonProperty(FIELD_NAME_COUNTS)
        private final RescalesCounts counts;

        @JsonProperty(FIELD_NAME_RESCALE_DURATION)
        private final StatsSummaryDto durationStats;

        @JsonProperty(FIELD_NAME_RESCALE_COMPLETED_DURATION)
        private final StatsSummaryDto completedDurationStats;

        @JsonProperty(FIELD_NAME_RESCALE_IGNORED_DURATION)
        private final StatsSummaryDto ignoredDurationStats;

        @JsonProperty(FIELD_NAME_RESCALE_FAILED_DURATION)
        private final StatsSummaryDto failedDurationStats;

        @JsonCreator
        public RescalesSummary(
                @JsonProperty(FIELD_NAME_COUNTS) RescalesCounts counts,
                @JsonProperty(FIELD_NAME_RESCALE_DURATION) StatsSummaryDto durationStats,
                @JsonProperty(FIELD_NAME_RESCALE_COMPLETED_DURATION)
                        StatsSummaryDto completedDurationStats,
                @JsonProperty(FIELD_NAME_RESCALE_IGNORED_DURATION)
                        StatsSummaryDto ignoredDurationStats,
                @JsonProperty(FIELD_NAME_RESCALE_FAILED_DURATION)
                        StatsSummaryDto failedDurationStats) {
            this.counts = counts;
            this.durationStats = durationStats;
            this.completedDurationStats = completedDurationStats;
            this.ignoredDurationStats = ignoredDurationStats;
            this.failedDurationStats = failedDurationStats;
        }

        public static RescalesSummary fromRescalesSummarySnapshot(
                RescalesSummarySnapshot snapshot) {
            RescalesCounts rescalesCounts =
                    new RescalesCounts(
                            snapshot.getIgnoredRescalesCount(),
                            snapshot.getPendingRescalesCount(),
                            snapshot.getCompletedRescalesCount(),
                            snapshot.getFailedRescalesCount(),
                            snapshot.getTotalRescalesCount());
            return new RescalesSummary(
                    rescalesCounts,
                    StatsSummaryDto.valueOf(snapshot.getAllSealedSummarySnapshot()),
                    StatsSummaryDto.valueOf(snapshot.getCompletedRescalesSummarySnapshot()),
                    StatsSummaryDto.valueOf(snapshot.getIgnoredRescalesSummarySnapshot()),
                    StatsSummaryDto.valueOf(snapshot.getFailedRescalesSummarySnapshot()));
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RescalesSummary that = (RescalesSummary) o;
            return Objects.equals(counts, that.counts)
                    && Objects.equals(durationStats, that.durationStats)
                    && Objects.equals(completedDurationStats, that.completedDurationStats)
                    && Objects.equals(ignoredDurationStats, that.ignoredDurationStats)
                    && Objects.equals(failedDurationStats, that.failedDurationStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    counts,
                    durationStats,
                    completedDurationStats,
                    ignoredDurationStats,
                    failedDurationStats);
        }
    }

    public static class RescalesCounts {
        public static final String FIELD_NAME_IGNORED_RESCALES = "ignored_rescales";
        public static final String FIELD_NAME_PENDING_RESCALES = "pending_rescales";
        public static final String FIELD_NAME_COMPLETED_RESCALES = "completed_rescales";
        public static final String FIELD_NAME_FAILED_RESCALES = "failed_rescales";
        public static final String FIELD_NAME_TOTAL_RESCALES = "total_rescales";

        @JsonProperty(FIELD_NAME_IGNORED_RESCALES)
        private final long ignoredRescales;

        @JsonProperty(FIELD_NAME_PENDING_RESCALES)
        private final long pendingRescales;

        @JsonProperty(FIELD_NAME_COMPLETED_RESCALES)
        private final long completedRescales;

        @JsonProperty(FIELD_NAME_FAILED_RESCALES)
        private final long failedRescales;

        @JsonProperty(FIELD_NAME_TOTAL_RESCALES)
        private final long totalRescales;

        @JsonCreator
        public RescalesCounts(
                @JsonProperty(FIELD_NAME_IGNORED_RESCALES) long ignoredRescales,
                @JsonProperty(FIELD_NAME_PENDING_RESCALES) long pendingRescales,
                @JsonProperty(FIELD_NAME_COMPLETED_RESCALES) long completedRescales,
                @JsonProperty(FIELD_NAME_FAILED_RESCALES) long failedRescales,
                @JsonProperty(FIELD_NAME_TOTAL_RESCALES) long totalRescales) {
            this.ignoredRescales = ignoredRescales;
            this.pendingRescales = pendingRescales;
            this.completedRescales = completedRescales;
            this.failedRescales = failedRescales;
            this.totalRescales = totalRescales;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RescalesCounts that = (RescalesCounts) o;
            return ignoredRescales == that.ignoredRescales
                    && pendingRescales == that.pendingRescales
                    && completedRescales == that.completedRescales
                    && failedRescales == that.failedRescales
                    && totalRescales == that.totalRescales;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    ignoredRescales,
                    pendingRescales,
                    completedRescales,
                    failedRescales,
                    totalRescales);
        }
    }

    public static class LatestRescales implements Serializable {
        private static final long serialVersionUID = 1L;
        public static final String FIELD_NAME_COMPLETED = "completed";
        public static final String FIELD_NAME_FAILED = "failed";
        public static final String FIELD_NAME_IGNORED = "ignored";

        @Nullable
        @JsonProperty(FIELD_NAME_COMPLETED)
        private final JobRescaleStatisticsDetails completed;

        @Nullable
        @JsonProperty(FIELD_NAME_FAILED)
        private final JobRescaleStatisticsDetails failed;

        @Nullable
        @JsonProperty(FIELD_NAME_IGNORED)
        private final JobRescaleStatisticsDetails ignored;

        @JsonCreator
        public LatestRescales(
                @Nullable @JsonProperty(FIELD_NAME_COMPLETED) JobRescaleStatisticsDetails completed,
                @Nullable @JsonProperty(FIELD_NAME_FAILED) JobRescaleStatisticsDetails failed,
                @Nullable @JsonProperty(FIELD_NAME_IGNORED) JobRescaleStatisticsDetails ignored) {
            this.completed = completed;
            this.failed = failed;
            this.ignored = ignored;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LatestRescales that = (LatestRescales) o;
            return Objects.equals(completed, that.completed)
                    && Objects.equals(failed, that.failed)
                    && Objects.equals(ignored, that.ignored);
        }

        @Override
        public int hashCode() {
            return Objects.hash(completed, failed, ignored);
        }

        public static LatestRescales fromRescalesStatsSnapshot(RescalesStatsSnapshot snapshot) {
            Rescale completedRescale = snapshot.getLatestRescales().get(TerminalState.COMPLETED);
            Rescale failedRescale = snapshot.getLatestRescales().get(TerminalState.FAILED);
            Rescale ignoredRescale = snapshot.getLatestRescales().get(TerminalState.IGNORED);
            return new LatestRescales(
                    Objects.isNull(completedRescale)
                            ? null
                            : JobRescaleStatisticsDetails.fromRescale(completedRescale, false),
                    Objects.isNull(failedRescale)
                            ? null
                            : JobRescaleStatisticsDetails.fromRescale(failedRescale, false),
                    Objects.isNull(ignoredRescale)
                            ? null
                            : JobRescaleStatisticsDetails.fromRescale(ignoredRescale, false));
        }
    }
}
