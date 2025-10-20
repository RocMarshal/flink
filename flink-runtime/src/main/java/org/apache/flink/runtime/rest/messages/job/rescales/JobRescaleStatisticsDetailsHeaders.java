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

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

public class JobRescaleStatisticsDetailsHeaders
        implements RuntimeMessageHeaders<
                EmptyRequestBody, JobRescaleStatisticsDetails, JobIDRescaleIDParameters> {

    public static final JobRescaleStatisticsDetailsHeaders INSTANCE =
            new JobRescaleStatisticsDetailsHeaders();

    public static final String JOB_RESCALES_PATH = "/jobs/:jobid/rescales/details/:rescaleuuid";

    @Override
    public Class<JobRescaleStatisticsDetails> getResponseClass() {
        return JobRescaleStatisticsDetails.class;
    }

    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Return a job rescale statistics details.";
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public JobIDRescaleIDParameters getUnresolvedMessageParameters() {
        return new JobIDRescaleIDParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return JOB_RESCALES_PATH;
    }

    public static JobRescaleStatisticsDetailsHeaders getInstance() {
        return INSTANCE;
    }
}
