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

package org.apache.flink.runtime.rest.handler.job.rescales;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescalesStatistics;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesStatsSnapshot;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.guava33.com.google.common.cache.Cache;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class JobRescalesStatisticsHandler
        extends AbstractJobRescalesHandler<JobRescalesStatistics, JobMessageParameters> {

    public JobRescalesStatisticsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, JobRescalesStatistics, JobMessageParameters>
                    messageHeaders,
            Executor executor,
            Cache<JobID, CompletableFuture<RescalesStatsSnapshot>> jobRescaleStatsSnapshotCache) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                messageHeaders,
                executor,
                jobRescaleStatsSnapshotCache);
    }

    @Override
    protected JobRescalesStatistics handleRescalesStatsRequest(
            HandlerRequest<EmptyRequestBody> request, RescalesStatsSnapshot rescalesStatsSnapshot)
            throws RestHandlerException {
        return JobRescalesStatistics.fromRescalesStatsSnapshot(rescalesStatsSnapshot);
    }
}
