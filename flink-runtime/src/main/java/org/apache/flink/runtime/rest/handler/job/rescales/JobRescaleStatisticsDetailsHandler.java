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
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.rescales.JobIDRescaleIDParameters;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleStatisticsDetails;
import org.apache.flink.runtime.scheduler.adaptive.timeline.Rescale;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesStatsSnapshot;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava33.com.google.common.cache.Cache;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class JobRescaleStatisticsDetailsHandler
        extends AbstractJobRescalesHandler<JobRescaleStatisticsDetails, JobIDRescaleIDParameters> {

    private final RescaleCache rescaleCache;

    public JobRescaleStatisticsDetailsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, JobRescaleStatisticsDetails, JobIDRescaleIDParameters>
                    messageHeaders,
            Executor executor,
            Cache<JobID, CompletableFuture<RescalesStatsSnapshot>> jobRescaleStatsSnapshotCache,
            RescaleCache rescaleCache) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                messageHeaders,
                executor,
                jobRescaleStatsSnapshotCache);

        this.rescaleCache = Preconditions.checkNotNull(rescaleCache);
    }

    @Override
    protected JobRescaleStatisticsDetails handleRescalesStatsRequest(
            HandlerRequest<EmptyRequestBody> request, RescalesStatsSnapshot rescalesStatsSnapshot)
            throws RestHandlerException {

        JobID jobId = request.getPathParameter(JobIDPathParameter.class);

        if (rescalesStatsSnapshot == null) {
            throw new RestHandlerException(
                    "Adaptive rescales was not enabled for job " + jobId + '.',
                    HttpResponseStatus.NOT_FOUND);
        }

        long rescaleId = request.getPathParameter(JobRescaleIDPathParameter.class);
        Rescale rescale = rescalesStatsSnapshot.getRecentRescales().get(rescaleId);
        if (rescale != null) {
            rescaleCache.tryAdd(rescale);
        } else {
            rescale = rescaleCache.tryGet(rescaleId);
        }

        if (rescale == null) {
            throw new RestHandlerException(
                    "Could not find rescale statistics for rescale " + rescaleId + '.',
                    HttpResponseStatus.NOT_FOUND);
        }

        return JobRescaleStatisticsDetails.fromRescale(rescale, true);
    }
}
