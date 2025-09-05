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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.rest.messages.ResourceProfileInfo;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDKeyDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDKeySerializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDSerializer;
import org.apache.flink.runtime.rest.messages.json.SlotSharingGroupIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.SlotSharingGroupIDKeyDeserializer;
import org.apache.flink.runtime.rest.messages.json.SlotSharingGroupIDKeySerializer;
import org.apache.flink.runtime.rest.messages.json.SlotSharingGroupIDSerializer;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.adaptive.timeline.Rescale;
import org.apache.flink.runtime.scheduler.adaptive.timeline.SlotSharingGroupRescale;
import org.apache.flink.runtime.scheduler.adaptive.timeline.TriggerCause;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Job rescales statistics details class. */
@Schema(name = "JobRescaleStatisticsDetails")
public class JobRescaleStatisticsDetails implements ResponseBody, Serializable {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_RESCALE_ID = "rescale_id";
    public static final String FIELD_NAME_RESCALE_UID = "rescale_uid";
    public static final String FIELD_NAME_RESOURCE_REQUIREMENTS_EPOCH =
            "resource_requirements_epoch";
    public static final String FIELD_NAME_CURRENT_EPOCH_SUB_RESCALE_ID =
            "current_epoch_sub_rescale_id";
    public static final String FIELD_NAME_RESOURCE_REQUIREMENTS_CHANGED =
            "resource_requirements_changed";
    public static final String FIELD_NAME_PARALLELISMS = "parallelisms";
    public static final String FIELD_NAME_SLOTS = "slots";
    public static final String FIELD_NAME_SCHEDULER_STATES = "scheduler_states";
    public static final String FIELD_NAME_TRIGGER_TIMESTAMP = "trigger_timestamp";
    public static final String FIELD_NAME_END_TIMESTAMP = "end_timestamp";
    public static final String FIELD_NAME_DURATION = "duration";
    public static final String FIELD_NAME_STATUS = "status";
    public static final String FIELD_NAME_TRIGGER_CAUSE = "trigger_cause";
    public static final String FIELD_NAME_SEALED_DESCRIPTION = "sealed_description";

    @JsonProperty(FIELD_NAME_RESCALE_ID)
    private final long rescaleId;

    @JsonProperty(FIELD_NAME_RESCALE_UID)
    private final String rescaleUid;

    @JsonProperty(FIELD_NAME_RESOURCE_REQUIREMENTS_EPOCH)
    private final String resourceRequirementsEpoch;

    @JsonProperty(FIELD_NAME_CURRENT_EPOCH_SUB_RESCALE_ID)
    private final long subRescaleIdOfCurrentEpoch;

    @JsonProperty(FIELD_NAME_RESOURCE_REQUIREMENTS_CHANGED)
    private final boolean newResourceRequirement;

    @JsonProperty(FIELD_NAME_PARALLELISMS)
    @JsonSerialize(keyUsing = JobVertexIDKeySerializer.class)
    private final Map<JobVertexID, VertexParallelismRescaleInfo> parallelisms;

    @JsonProperty(FIELD_NAME_SLOTS)
    @JsonSerialize(keyUsing = SlotSharingGroupIDKeySerializer.class)
    private final Map<SlotSharingGroupId, SlotSharingGroupRescaleInfo> slots;

    @JsonProperty(FIELD_NAME_SCHEDULER_STATES)
    private final List<SchedulerStateSpan> schedulerStates;

    @JsonProperty(FIELD_NAME_TRIGGER_TIMESTAMP)
    private final Long startTimestamp;

    @JsonProperty(FIELD_NAME_END_TIMESTAMP)
    private final Long endTimestamp;

    @JsonProperty(FIELD_NAME_DURATION)
    private final Long duration;

    @JsonProperty(FIELD_NAME_STATUS)
    private final RescaleStatus status;

    @JsonProperty(FIELD_NAME_TRIGGER_CAUSE)
    private final TriggerCause triggerCause;

    @JsonProperty(FIELD_NAME_SEALED_DESCRIPTION)
    private final String sealedDescription;

    @JsonCreator
    public JobRescaleStatisticsDetails(
            @JsonProperty(FIELD_NAME_RESCALE_ID) long rescaleId,
            @JsonProperty(FIELD_NAME_RESCALE_UID) String rescaleUid,
            @JsonProperty(FIELD_NAME_RESOURCE_REQUIREMENTS_EPOCH) String resourceRequirementsEpoch,
            @JsonProperty(FIELD_NAME_CURRENT_EPOCH_SUB_RESCALE_ID) long subRescaleIdOfCurrentEpoch,
            @JsonProperty(FIELD_NAME_RESOURCE_REQUIREMENTS_CHANGED) boolean newResourceRequirement,
            @JsonDeserialize(keyUsing = JobVertexIDKeyDeserializer.class)
                    @JsonProperty(FIELD_NAME_PARALLELISMS)
                    Map<JobVertexID, VertexParallelismRescaleInfo> parallelisms,
            @JsonDeserialize(keyUsing = SlotSharingGroupIDKeyDeserializer.class)
                    @JsonProperty(FIELD_NAME_SLOTS)
                    Map<SlotSharingGroupId, SlotSharingGroupRescaleInfo> slots,
            @JsonProperty(FIELD_NAME_SCHEDULER_STATES) List<SchedulerStateSpan> schedulerStates,
            @JsonProperty(FIELD_NAME_TRIGGER_TIMESTAMP) Long startTimestamp,
            @JsonProperty(FIELD_NAME_END_TIMESTAMP) Long endTimestamp,
            @JsonProperty(FIELD_NAME_DURATION) Long duration,
            @JsonProperty(FIELD_NAME_STATUS) RescaleStatus status,
            @JsonProperty(FIELD_NAME_TRIGGER_CAUSE) TriggerCause triggerCause,
            @JsonProperty(FIELD_NAME_SEALED_DESCRIPTION) String sealedDescription) {
        this.rescaleId = rescaleId;
        this.rescaleUid = rescaleUid;
        this.resourceRequirementsEpoch = resourceRequirementsEpoch;
        this.subRescaleIdOfCurrentEpoch = subRescaleIdOfCurrentEpoch;
        this.newResourceRequirement = newResourceRequirement;
        this.parallelisms = parallelisms;
        this.slots = slots;
        this.schedulerStates = schedulerStates;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.duration = duration;
        this.status = status;
        this.triggerCause = triggerCause;
        this.sealedDescription = sealedDescription;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobRescaleStatisticsDetails that = (JobRescaleStatisticsDetails) o;
        return rescaleId == that.rescaleId
                && subRescaleIdOfCurrentEpoch == that.subRescaleIdOfCurrentEpoch
                && newResourceRequirement == that.newResourceRequirement
                && Objects.equals(rescaleUid, that.rescaleUid)
                && Objects.equals(resourceRequirementsEpoch, that.resourceRequirementsEpoch)
                && Objects.equals(parallelisms, that.parallelisms)
                && Objects.equals(slots, that.slots)
                && Objects.equals(schedulerStates, that.schedulerStates)
                && Objects.equals(startTimestamp, that.startTimestamp)
                && Objects.equals(endTimestamp, that.endTimestamp)
                && Objects.equals(duration, that.duration)
                && status == that.status
                && triggerCause == that.triggerCause
                && Objects.equals(sealedDescription, that.sealedDescription);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rescaleId,
                rescaleUid,
                resourceRequirementsEpoch,
                subRescaleIdOfCurrentEpoch,
                newResourceRequirement,
                parallelisms,
                slots,
                schedulerStates,
                startTimestamp,
                endTimestamp,
                duration,
                status,
                triggerCause,
                sealedDescription);
    }

    public static JobRescaleStatisticsDetails fromRescale(
            Rescale rescale, boolean includeSchedulerStates) {
        return new JobRescaleStatisticsDetails(
                rescale.getRescaleIdInfo().getRescaleUuid(),
                rescale.getRescaleIdInfo().getRescaleUuid().toString(),
                rescale.getRescaleIdInfo().getResourceRequirementsId().toString(),
                rescale.getRescaleIdInfo().getRescaleAttemptId(),
                rescale.getParallelisms(),
                convertMapValues(
                        rescale.getSlots(),
                        SlotSharingGroupRescaleInfo::fromSlotSharingGroupRescale),
                includeSchedulerStates ? rescale.getSchedulerStates() : null,
                rescale.getStartTimestamp(),
                rescale.getEndTimestamp(),
                rescale.getDuration().toMillis(),
                rescale.getStatus(),
                rescale.getTriggerCause(),
                rescale.getSealedDescription());
    }

    private static <K, NV, OV> Map<K, NV> convertMapValues(
            Map<K, OV> rawMap, Function<OV, NV> valueMapper) {
        return rawMap == null
                ? new HashMap<>()
                : rawMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        kovEntry -> valueMapper.apply(kovEntry.getValue())));
    }

    public static final class VertexParallelismRescaleInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        public static final String FIELD_NAME_JOB_VERTEX_ID = "job_vertex_id";
        public static final String FIELD_NAME_VERTEX_NAME = "job_vertex_name";
        public static final String FIELD_NAME_SLOT_SHARING_GROUP_ID = "slot_sharing_group_id";
        public static final String FIELD_NAME_SLOT_SHARING_GROUP_NAME = "slot_sharing_group_name";
        public static final String FIELD_NAME_REQUIRED_PARALLELISM = "required_parallelism";
        public static final String FIELD_NAME_REQUIRED_PARALLELISM_LOWER_BOUND =
                "required_parallelism_lower_bound";
        public static final String FIELD_NAME_REQUIRED_PARALLELISM_UPPER_BOUND =
                "required_parallelism_upper_bound";
        public static final String FIELD_NAME_CURRENT_PARALLELISM = "current_parallelism";
        public static final String FIELD_NAME_ACQUIRED_PARALLELISM = "acquired_parallelism";

        @JsonProperty(FIELD_NAME_JOB_VERTEX_ID)
        @JsonSerialize(using = JobVertexIDSerializer.class)
        private final JobVertexID jobVertexId;

        @JsonProperty(FIELD_NAME_VERTEX_NAME)
        private String jobVertexName;

        @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_ID)
        @JsonSerialize(using = SlotSharingGroupIDSerializer.class)
        private SlotSharingGroupId slotSharingGroupId;

        @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_NAME)
        private String slotSharingGroupName;

        @JsonProperty(FIELD_NAME_REQUIRED_PARALLELISM)
        private Integer requiredParallelism;

        @JsonProperty(FIELD_NAME_REQUIRED_PARALLELISM_LOWER_BOUND)
        private Integer requiredParallelismLowerBound;

        @JsonProperty(FIELD_NAME_REQUIRED_PARALLELISM_UPPER_BOUND)
        private Integer requiredParallelismUpperBound;

        @Nullable
        @JsonProperty(FIELD_NAME_CURRENT_PARALLELISM)
        private Integer currentParallelism;

        @Nullable
        @JsonProperty(FIELD_NAME_ACQUIRED_PARALLELISM)
        private Integer acquiredParallelism;

        @JsonIgnore
        public VertexParallelismRescaleInfo(JobVertexID jobVertexId) {
            this.jobVertexId = Preconditions.checkNotNull(jobVertexId);
        }

        @JsonCreator
        public VertexParallelismRescaleInfo(
                @JsonDeserialize(using = JobVertexIDDeserializer.class)
                        @JsonProperty(FIELD_NAME_JOB_VERTEX_ID)
                        JobVertexID jobVertexId,
                @JsonProperty(FIELD_NAME_VERTEX_NAME) String jobVertexName,
                @JsonDeserialize(using = SlotSharingGroupIDDeserializer.class)
                        @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_ID)
                        SlotSharingGroupId slotSharingGroupId,
                @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_NAME) String slotSharingGroupName,
                @JsonProperty(FIELD_NAME_REQUIRED_PARALLELISM) Integer requiredParallelism,
                @JsonProperty(FIELD_NAME_REQUIRED_PARALLELISM_LOWER_BOUND)
                        Integer requiredParallelismLowerBound,
                @JsonProperty(FIELD_NAME_REQUIRED_PARALLELISM_UPPER_BOUND)
                        Integer requiredParallelismUpperBound,
                @Nullable @JsonProperty(FIELD_NAME_CURRENT_PARALLELISM) Integer currentParallelism,
                @JsonProperty(FIELD_NAME_ACQUIRED_PARALLELISM) Integer acquiredParallelism) {
            this.jobVertexId = jobVertexId;
            this.jobVertexName = jobVertexName;
            this.slotSharingGroupId = slotSharingGroupId;
            this.slotSharingGroupName = slotSharingGroupName;
            this.requiredParallelism = requiredParallelism;
            this.requiredParallelismLowerBound = requiredParallelismLowerBound;
            this.requiredParallelismUpperBound = requiredParallelismUpperBound;
            this.currentParallelism = currentParallelism;
            this.acquiredParallelism = acquiredParallelism;
        }

        @JsonIgnore
        public void setJobVertexName(String jobVertexName) {
            this.jobVertexName = jobVertexName;
        }

        @JsonIgnore
        public void setCurrentParallelism(@Nullable Integer currentParallelism) {
            this.currentParallelism = currentParallelism;
        }

        @JsonIgnore
        @Nullable
        public Integer getAcquiredParallelism() {
            return acquiredParallelism;
        }

        @JsonIgnore
        public void setAcquiredParallelism(Integer acquiredParallelism) {
            this.acquiredParallelism = acquiredParallelism;
        }

        @JsonIgnore
        public void setSlotSharingGroupMetaInfo(SlotSharingGroup slotSharingGroup) {
            this.slotSharingGroupName = slotSharingGroup.getSlotSharingGroupName();
            this.slotSharingGroupId = slotSharingGroup.getSlotSharingGroupId();
        }

        @JsonIgnore
        public void setRequiredParallelismWithBounds(
                VertexParallelismInformation vertexParallelismInformation) {
            this.requiredParallelismLowerBound = vertexParallelismInformation.getMinParallelism();
            this.requiredParallelismUpperBound = vertexParallelismInformation.getMaxParallelism();
            this.requiredParallelism = vertexParallelismInformation.getParallelism();
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            VertexParallelismRescaleInfo that = (VertexParallelismRescaleInfo) o;
            return Objects.equals(jobVertexId, that.jobVertexId)
                    && Objects.equals(jobVertexName, that.jobVertexName)
                    && Objects.equals(slotSharingGroupId, that.slotSharingGroupId)
                    && Objects.equals(slotSharingGroupName, that.slotSharingGroupName)
                    && Objects.equals(requiredParallelism, that.requiredParallelism)
                    && Objects.equals(
                            requiredParallelismLowerBound, that.requiredParallelismLowerBound)
                    && Objects.equals(
                            requiredParallelismUpperBound, that.requiredParallelismUpperBound)
                    && Objects.equals(currentParallelism, that.currentParallelism)
                    && Objects.equals(acquiredParallelism, that.acquiredParallelism);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    jobVertexId,
                    jobVertexName,
                    slotSharingGroupId,
                    slotSharingGroupName,
                    requiredParallelism,
                    requiredParallelismLowerBound,
                    requiredParallelismUpperBound,
                    currentParallelism,
                    acquiredParallelism);
        }

        @Override
        public String toString() {
            return "VertexParallelismRescale{"
                    + "jobVertexId='"
                    + jobVertexId
                    + '\''
                    + ", jobVertexName='"
                    + jobVertexName
                    + '\''
                    + ", slotSharingGroupId='"
                    + slotSharingGroupId
                    + '\''
                    + ", slotSharingGroupName='"
                    + slotSharingGroupName
                    + '\''
                    + ", currentParallelism="
                    + currentParallelism
                    + ", requiredParallelism="
                    + requiredParallelism
                    + ", requiredParallelismLowerBound="
                    + requiredParallelismLowerBound
                    + ", requiredParallelismUpperBound="
                    + requiredParallelismUpperBound
                    + ", acquiredParallelism="
                    + acquiredParallelism
                    + '}';
        }
    }

    public static final class SlotSharingGroupRescaleInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        public static final String FIELD_NAME_SLOT_SHARING_GROUP_ID = "slot_sharing_group_id";
        public static final String FIELD_NAME_SLOT_SHARING_GROUP_NAME = "slot_sharing_group_name";
        public static final String FIELD_NAME_REQUEST_RESOURCE_PROFILE = "request_resource_profile";
        public static final String FIELD_NAME_DESIRED_SLOTS = "desired_slots";
        public static final String FIELD_NAME_SUFFICIENT_SLOTS = "sufficient_slots";
        public static final String FIELD_NAME_CURRENT_SLOTS = "current_slots";
        public static final String FIELD_NAME_ACQUIRED_SLOTS = "acquired_slots";
        public static final String FIELD_NAME_ACQUIRED_RESOURCE_PROFILE =
                "acquired_resource_profile";

        @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_ID)
        @JsonSerialize(using = SlotSharingGroupIDSerializer.class)
        private final SlotSharingGroupId slotSharingGroupId;

        @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_NAME)
        private final String slotSharingGroupName;

        @JsonProperty(FIELD_NAME_REQUEST_RESOURCE_PROFILE)
        private final ResourceProfileInfo requiredResourceProfileInfo;

        @JsonProperty(FIELD_NAME_DESIRED_SLOTS)
        private final Integer desiredSlots;

        @JsonProperty(FIELD_NAME_SUFFICIENT_SLOTS)
        private final Integer sufficientSlots;

        @JsonProperty(FIELD_NAME_CURRENT_SLOTS)
        private final Integer currentSlots;

        @JsonProperty(FIELD_NAME_ACQUIRED_SLOTS)
        private final Integer acquiredSlots;

        @JsonProperty(FIELD_NAME_ACQUIRED_RESOURCE_PROFILE)
        private final ResourceProfileInfo acquiredResourceProfileInfo;

        @JsonCreator
        public SlotSharingGroupRescaleInfo(
                @JsonDeserialize(using = SlotSharingGroupIDDeserializer.class)
                        @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_ID)
                        SlotSharingGroupId slotSharingGroupId,
                @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_NAME) String slotSharingGroupName,
                @JsonProperty(FIELD_NAME_REQUEST_RESOURCE_PROFILE)
                        ResourceProfileInfo requiredResourceProfileInfo,
                @JsonProperty(FIELD_NAME_DESIRED_SLOTS) Integer desiredSlots,
                @JsonProperty(FIELD_NAME_SUFFICIENT_SLOTS) Integer sufficientSlots,
                @JsonProperty(FIELD_NAME_CURRENT_SLOTS) Integer currentSlots,
                @JsonProperty(FIELD_NAME_ACQUIRED_SLOTS) Integer acquiredSlots,
                @JsonProperty(FIELD_NAME_ACQUIRED_RESOURCE_PROFILE)
                        ResourceProfileInfo acquiredResourceProfileInfo) {
            this.slotSharingGroupId = slotSharingGroupId;
            this.slotSharingGroupName = slotSharingGroupName;
            this.requiredResourceProfileInfo = requiredResourceProfileInfo;
            this.desiredSlots = desiredSlots;
            this.sufficientSlots = sufficientSlots;
            this.currentSlots = currentSlots;
            this.acquiredSlots = acquiredSlots;
            this.acquiredResourceProfileInfo = acquiredResourceProfileInfo;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SlotSharingGroupRescaleInfo that = (SlotSharingGroupRescaleInfo) o;
            return Objects.equals(slotSharingGroupId, that.slotSharingGroupId)
                    && Objects.equals(slotSharingGroupName, that.slotSharingGroupName)
                    && Objects.equals(requiredResourceProfileInfo, that.requiredResourceProfileInfo)
                    && Objects.equals(desiredSlots, that.desiredSlots)
                    && Objects.equals(sufficientSlots, that.sufficientSlots)
                    && Objects.equals(currentSlots, that.currentSlots)
                    && Objects.equals(acquiredSlots, that.acquiredSlots)
                    && Objects.equals(
                            acquiredResourceProfileInfo, that.acquiredResourceProfileInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    slotSharingGroupId,
                    slotSharingGroupName,
                    requiredResourceProfileInfo,
                    desiredSlots,
                    sufficientSlots,
                    currentSlots,
                    acquiredSlots,
                    acquiredResourceProfileInfo);
        }

        public static SlotSharingGroupRescaleInfo fromSlotSharingGroupRescale(
                SlotSharingGroupRescale slotSharingGroupRescale) {
            return new SlotSharingGroupRescaleInfo(
                    slotSharingGroupRescale.getSlotSharingGroupId(),
                    slotSharingGroupRescale.getSlotSharingGroupName(),
                    ResourceProfileInfo.fromResourceProfile(
                            slotSharingGroupRescale.getRequiredResourceProfile()),
                    slotSharingGroupRescale.getDesiredSlots(),
                    slotSharingGroupRescale.getMinimalRequiredSlots(),
                    slotSharingGroupRescale.getPreRescaleSlots(),
                    slotSharingGroupRescale.getPostRescaleSlots(),
                    ResourceProfileInfo.fromResourceProfile(
                            Optional.ofNullable(
                                            slotSharingGroupRescale.getAcquiredResourceProfile())
                                    .orElse(ResourceProfile.UNKNOWN)));
        }
    }
}
