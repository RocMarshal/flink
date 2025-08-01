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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleStatisticsDetails.VertexParallelismRescaleInfo;
import org.apache.flink.runtime.rest.messages.job.rescales.SchedulerStateSpan;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan;
import org.apache.flink.runtime.scheduler.adaptive.State;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.SlotSharingGroupMetaInfo;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Rescale event. */
public class Rescale implements Serializable {

    public static final Logger LOG = LoggerFactory.getLogger(Rescale.class);

    @Nullable private transient String stringedException;

    private final IdEpoch idEpoch;

    private final boolean newResourceRequirement;
    private final Map<JobVertexID, VertexParallelismRescaleInfo> parallelisms;
    private final Map<SlotSharingGroupId, SlotSharingGroupRescale> slots;

    private final List<SchedulerStateSpan> schedulerStates;

    private Long startTimestamp;
    private Long endTimestamp;
    private RescaleStatus status = RescaleStatus.UNKNOWN;

    private TriggerCause triggerCause;
    private String sealedDescription;

    Rescale(IdEpoch idEpoch, boolean newResourceRequirement) {
        this.idEpoch = idEpoch;
        this.newResourceRequirement = newResourceRequirement;
        this.parallelisms = new HashMap<>();
        this.slots = new HashMap<>();
        this.schedulerStates = new ArrayList<>();
        LOG.info("Generated a new {} rescale {}", status, this);
    }

    public boolean isNewResourceRequirement() {
        return newResourceRequirement;
    }

    public IdEpoch getIdEpoch() {
        return idEpoch;
    }

    private Rescale addSchedulerState(SchedulerStateSpan schedulerStateSpan) {
        if (isSealed()) {
            LOG.warn(
                    "Rescale is already sealed. The scheduler state {} will be ignored.",
                    schedulerStateSpan);
            return this;
        }
        this.schedulerStates.add(schedulerStateSpan);
        return this;
    }

    public Rescale addSchedulerState(State state) {
        return addSchedulerState(state, null);
    }

    public Rescale addSchedulerState(State schedulerState, @Nullable Throwable throwable) {
        Long outTimestamp = schedulerState.getDurable().getOutTimestamp();
        long logicEndMillis =
                Objects.isNull(outTimestamp) ? Instant.now().toEpochMilli() : outTimestamp;
        String exceptionStr =
                Objects.isNull(throwable)
                        ? stringedException
                        : ExceptionUtils.stringifyException(throwable);
        if (stringedException != null) {
            stringedException = null;
        }
        return addSchedulerState(
                new SchedulerStateSpan(
                        schedulerState.getClass().getSimpleName(),
                        schedulerState.getDurable().getInTimestamp(),
                        logicEndMillis,
                        logicEndMillis - schedulerState.getDurable().getInTimestamp(),
                        exceptionStr));
    }

    public Rescale setStatus(Optional<RescaleStatus> statusOptional) {
        return setStatus(statusOptional.orElse(RescaleStatus.UNKNOWN));
    }

    public Rescale setStatus(RescaleStatus status) {
        if (status == RescaleStatus.UNKNOWN) {
            LOG.warn("Received unknown status from scheduler state.");
            return this;
        }
        if (this.status.isSealed()) {
            LOG.warn("The old status was already set to {}", this.status);
        }
        this.status = Preconditions.checkNotNull(status);
        return this;
    }

    public Duration getDuration() {
        if (this.status.isSealed() && startTimestamp != null && endTimestamp != null) {
            return Duration.ofMillis(endTimestamp - startTimestamp);
        }
        return Duration.ZERO;
    }

    public Rescale setSealedDescription(String sealedDescription) {
        if (this.sealedDescription != null) {
            LOG.warn("The old sealed description was already set to '{}'", this.sealedDescription);
        }
        this.sealedDescription = sealedDescription;
        return this;
    }

    public Rescale setStartTimestamp(long timestamp) {
        if (this.startTimestamp != null) {
            LOG.warn("The old startTimestamp was already set to '{}'", this.startTimestamp);
        }
        this.startTimestamp = timestamp;
        return this;
    }

    public Rescale setEndTimestamp(Long endTimestamp) {
        if (this.endTimestamp != null) {
            LOG.warn("The old endTimestamp was already set to '{}'", this.endTimestamp);
        }
        this.endTimestamp = endTimestamp;
        return this;
    }

    public RescaleStatus getStatus() {
        return status;
    }

    public Rescale setRequiredSlots(JobInformation jobInformation) {
        for (SlotSharingGroup sharingGroup : jobInformation.getSlotSharingGroups()) {
            int requiredSlots =
                    sharingGroup.getJobVertexIds().stream()
                            .map(
                                    jobVertexID ->
                                            jobInformation
                                                    .getVertexInformation(jobVertexID)
                                                    .getParallelism())
                            .max(Integer::compare)
                            .orElse(0);
            SlotSharingGroupId sharingGroupId = sharingGroup.getSlotSharingGroupId();
            SlotSharingGroupRescale sharingGroupRescaleInfo =
                    slots.computeIfAbsent(sharingGroupId, SlotSharingGroupRescale::new);
            sharingGroupRescaleInfo.setSlotSharingGroupMetaInfo(sharingGroup);
            sharingGroupRescaleInfo.setDesiredSlots(requiredSlots);
        }
        return this;
    }

    public Rescale setAcquiredSlots(Collection<JobSchedulingPlan.SlotAssignment> slotAssignments) {
        Map<SlotSharingGroupId, Set<JobSchedulingPlan.SlotAssignment>> assignmentsPerSharingGroup =
                slotAssignments.stream()
                        .collect(
                                Collectors.groupingBy(
                                        slotAssignment ->
                                                slotAssignment
                                                        .getTargetAs(
                                                                ExecutionSlotSharingGroup.class)
                                                        .getSlotSharingGroup()
                                                        .getSlotSharingGroupId(),
                                        Collectors.toSet()));
        for (Map.Entry<SlotSharingGroupId, Set<JobSchedulingPlan.SlotAssignment>> entry :
                assignmentsPerSharingGroup.entrySet()) {
            SlotSharingGroupId sharingGroupId = entry.getKey();
            Set<JobSchedulingPlan.SlotAssignment> assignments =
                    assignmentsPerSharingGroup.get(sharingGroupId);
            int acquiredSlots = assignments.size();
            ResourceProfile acquiredResource =
                    assignments.iterator().next().getSlotInfo().getResourceProfile();
            SlotSharingGroupRescale slotSharingGroupRescale = slots.get(sharingGroupId);
            slotSharingGroupRescale.setAcquiredSlots(acquiredSlots);
            slotSharingGroupRescale.setAcquiredResourceProfile(acquiredResource);
        }
        return this;
    }

    public Rescale setSufficientSlots(JobInformation jobInformation) {
        final Map<SlotSharingGroup, SlotSharingGroupMetaInfo> slotSharingGroupMetaInfo =
                SlotSharingGroupMetaInfo.from(jobInformation.getVertices());
        for (Map.Entry<SlotSharingGroup, SlotSharingGroupMetaInfo> entry :
                slotSharingGroupMetaInfo.entrySet()) {
            SlotSharingGroupId groupId = entry.getKey().getSlotSharingGroupId();
            SlotSharingGroupRescale slotSharingGroupRescale =
                    slots.computeIfAbsent(groupId, SlotSharingGroupRescale::new);
            slotSharingGroupRescale.setSufficientSlots(entry.getValue().getMaxLowerBound());
        }
        return this;
    }

    public Rescale setCurrentSlotsAndParallelisms(Rescale lastCompletedRescale) {
        if (lastCompletedRescale == null) {
            LOG.info("No available previous parallelism to set.");
            return this;
        }
        for (JobVertexID jobVertexID : parallelisms.keySet()) {
            Integer previousAcquiredParallelism =
                    lastCompletedRescale.parallelisms.get(jobVertexID).getAcquiredParallelism();
            VertexParallelismRescaleInfo vertexParallelismRescaleInfo =
                    parallelisms.computeIfAbsent(jobVertexID, VertexParallelismRescaleInfo::new);
            vertexParallelismRescaleInfo.setCurrentParallelism(previousAcquiredParallelism);
        }

        for (SlotSharingGroupId sharingGroupId : slots.keySet()) {
            Integer previousAcquiredSlot =
                    lastCompletedRescale.slots.get(sharingGroupId).getAcquiredSlots();
            SlotSharingGroupRescale slotSharingGroupRescale =
                    slots.computeIfAbsent(sharingGroupId, SlotSharingGroupRescale::new);
            slotSharingGroupRescale.setCurrentSlots(previousAcquiredSlot);
        }

        return this;
    }

    public Rescale setRequiredVertexParallelism(JobInformation jobInformation) {
        Map<JobVertexID, VertexParallelismInformation> allParallelismInfo =
                jobInformation.getVertexParallelismStore().getAllParallelismInfo();
        for (Map.Entry<JobVertexID, VertexParallelismInformation> entry :
                allParallelismInfo.entrySet()) {
            JobVertexID jvId = entry.getKey();
            VertexParallelismInformation vertexParallelInfo = entry.getValue();
            VertexParallelismRescaleInfo vertexParallelismRescaleInfo =
                    this.parallelisms.computeIfAbsent(
                            jvId, jobVertexID -> new VertexParallelismRescaleInfo(jvId));
            SlotSharingGroup slotSharingGroup =
                    jobInformation.getVertexInformation(jvId).getSlotSharingGroup();
            vertexParallelismRescaleInfo.setSlotSharingGroupMetaInfo(slotSharingGroup);
            vertexParallelismRescaleInfo.setJobVertexName(jobInformation.getVertexName(jvId));
            vertexParallelismRescaleInfo.setRequiredParallelismWithBounds(vertexParallelInfo);
        }
        return this;
    }

    public boolean isSealed() {
        return status.isSealed() && startTimestamp != null && endTimestamp != null;
    }

    public Rescale setAcquiredVertexParallelism(VertexParallelism acquiredVertexParallelism) {
        Set<JobVertexID> vertices = acquiredVertexParallelism.getVertices();
        for (JobVertexID vertexID : vertices) {
            VertexParallelismRescaleInfo vertexParallelismRescaleInfo =
                    this.parallelisms.get(vertexID);
            vertexParallelismRescaleInfo.setAcquiredParallelism(
                    acquiredVertexParallelism.getParallelism(vertexID));
        }
        return this;
    }

    public Rescale setTriggerCause(TriggerCause triggerCause) {
        this.triggerCause = triggerCause;
        return this;
    }

    public void log() {
        LOG.info("Updated a {} rescale {}", status, this);
    }

    public Map<JobVertexID, VertexParallelismRescaleInfo> getParallelisms() {
        return parallelisms;
    }

    public Map<SlotSharingGroupId, SlotSharingGroupRescale> getSlots() {
        return slots;
    }

    public List<SchedulerStateSpan> getSchedulerStates() {
        return schedulerStates;
    }

    public Long getStartTimestamp() {
        return startTimestamp;
    }

    public Long getEndTimestamp() {
        return endTimestamp;
    }

    public TriggerCause getTriggerCause() {
        return triggerCause;
    }

    public String getSealedDescription() {
        return sealedDescription;
    }

    public static boolean isSealed(Rescale rescale) {
        return rescale != null && rescale.isSealed();
    }

    @Override
    public String toString() {
        return "Rescale{"
                + "idEpoch="
                + idEpoch
                + ", newResourceRequirement="
                + newResourceRequirement
                + ", parallelisms="
                + parallelisms
                + ", slots="
                + slots
                + ", schedulerStates="
                + schedulerStates
                + ", startTimestamp="
                + startTimestamp
                + ", endTimestamp="
                + endTimestamp
                + ", status="
                + status
                + ", triggerCause="
                + triggerCause
                + ", sealedDescription='"
                + sealedDescription
                + "'}";
    }

    public void setStringedException(String stringedException) {
        this.stringedException = stringedException;
    }
}
