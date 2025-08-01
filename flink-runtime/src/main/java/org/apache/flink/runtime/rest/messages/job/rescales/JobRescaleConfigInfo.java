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

import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Objects;

/** Configuration information related to rescaling for jobs with the adaptive scheduler enabled. */
@Schema(name = "JobRescaleConfigInfo")
public class JobRescaleConfigInfo implements ResponseBody {

    public static final String FIELD_NAME_RESCALE_HISTORY_MAX = "rescale_history_max";
    public static final String FIELD_NAME_SCHEDULER_EXECUTION_MODE = "scheduler_execution_mode";
    public static final String FIELD_NAME_SUBMISSION_RESOURCE_WAIT_TIMEOUT =
            "submission_resource_wait_timeout";
    public static final String FIELD_NAME_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT =
            "submission_resource_stabilization_timeout";
    public static final String FIELD_NAME_SLOT_IDLE_TIMEOUT = "slot_idle_timeout";
    public static final String FIELD_NAME_EXECUTING_COOLDOWN_TIMEOUT = "executing_cooldown_timeout";
    public static final String FIELD_NAME_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT =
            "executing_resource_stabilization_timeout";
    public static final String FIELD_NAME_MAXIMUM_DELAY_FOR_TRIGGERING_RESCALE =
            "maximum_delay_for_triggering_rescale";
    public static final String FIELD_NAME_RESCALE_ON_FAILED_CHECKPOINT_COUNT =
            "rescale_on_failed_checkpoint_count";

    @JsonProperty(FIELD_NAME_RESCALE_HISTORY_MAX)
    private final Integer rescaleHistoryMax;

    @JsonProperty(FIELD_NAME_SCHEDULER_EXECUTION_MODE)
    private final SchedulerExecutionMode schedulerExecutionMode;

    @JsonProperty(FIELD_NAME_SUBMISSION_RESOURCE_WAIT_TIMEOUT)
    private final Long submissionResourceWaitTimeout;

    @JsonProperty(FIELD_NAME_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT)
    private final Long submissionResourceStabilizationTimeout;

    @JsonProperty(FIELD_NAME_SLOT_IDLE_TIMEOUT)
    private final Long slotIdleTimeout;

    @JsonProperty(FIELD_NAME_EXECUTING_COOLDOWN_TIMEOUT)
    private final Long executingCooldownTimeout;

    @JsonProperty(FIELD_NAME_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT)
    private final Long executingResourceStabilizationTimeout;

    @JsonProperty(FIELD_NAME_MAXIMUM_DELAY_FOR_TRIGGERING_RESCALE)
    private final Long maximumDelayForTriggeringRescale;

    @JsonProperty(FIELD_NAME_RESCALE_ON_FAILED_CHECKPOINT_COUNT)
    private final Integer rescaleOnFailedCheckpointCount;

    @JsonCreator
    public JobRescaleConfigInfo(
            @JsonProperty(FIELD_NAME_RESCALE_HISTORY_MAX) Integer rescaleHistoryMax,
            @JsonProperty(FIELD_NAME_SCHEDULER_EXECUTION_MODE)
                    SchedulerExecutionMode schedulerExecutionMode,
            @JsonProperty(FIELD_NAME_SUBMISSION_RESOURCE_WAIT_TIMEOUT)
                    Long submissionResourceWaitTimeout,
            @JsonProperty(FIELD_NAME_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT)
                    Long submissionResourceStabilizationTimeout,
            @JsonProperty(FIELD_NAME_SLOT_IDLE_TIMEOUT) Long slotIdleTimeout,
            @JsonProperty(FIELD_NAME_EXECUTING_COOLDOWN_TIMEOUT) Long executingCooldownTimeout,
            @JsonProperty(FIELD_NAME_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT)
                    Long executingResourceStabilizationTimeout,
            @JsonProperty(FIELD_NAME_MAXIMUM_DELAY_FOR_TRIGGERING_RESCALE)
                    Long maximumDelayForTriggeringRescale,
            @JsonProperty(FIELD_NAME_RESCALE_ON_FAILED_CHECKPOINT_COUNT)
                    Integer rescaleOnFailedCheckpointCount) {
        this.rescaleHistoryMax = rescaleHistoryMax;
        this.schedulerExecutionMode = schedulerExecutionMode;
        this.submissionResourceWaitTimeout = submissionResourceWaitTimeout;
        this.submissionResourceStabilizationTimeout = submissionResourceStabilizationTimeout;
        this.slotIdleTimeout = slotIdleTimeout;
        this.executingCooldownTimeout = executingCooldownTimeout;
        this.executingResourceStabilizationTimeout = executingResourceStabilizationTimeout;
        this.maximumDelayForTriggeringRescale = maximumDelayForTriggeringRescale;
        this.rescaleOnFailedCheckpointCount = rescaleOnFailedCheckpointCount;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JobRescaleConfigInfo that = (JobRescaleConfigInfo) o;
        return Objects.equals(rescaleHistoryMax, that.rescaleHistoryMax)
                && Objects.equals(schedulerExecutionMode, that.schedulerExecutionMode)
                && Objects.equals(submissionResourceWaitTimeout, that.submissionResourceWaitTimeout)
                && Objects.equals(
                        submissionResourceStabilizationTimeout,
                        that.submissionResourceStabilizationTimeout)
                && Objects.equals(slotIdleTimeout, that.slotIdleTimeout)
                && Objects.equals(executingCooldownTimeout, that.executingCooldownTimeout)
                && Objects.equals(
                        executingResourceStabilizationTimeout,
                        that.executingResourceStabilizationTimeout)
                && Objects.equals(
                        maximumDelayForTriggeringRescale, that.maximumDelayForTriggeringRescale)
                && Objects.equals(
                        rescaleOnFailedCheckpointCount, that.rescaleOnFailedCheckpointCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rescaleHistoryMax,
                schedulerExecutionMode,
                submissionResourceWaitTimeout,
                submissionResourceStabilizationTimeout,
                slotIdleTimeout,
                executingCooldownTimeout,
                executingResourceStabilizationTimeout,
                maximumDelayForTriggeringRescale,
                rescaleOnFailedCheckpointCount);
    }
}
