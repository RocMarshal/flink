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
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan;
import org.apache.flink.runtime.scheduler.adaptive.State;
import org.apache.flink.runtime.scheduler.adaptive.allocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingGroupMetaInfo;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Rescale event. */
public class Rescale implements Serializable {

    private final transient RescaleTimeline timeline;
    private final transient Logger log;

    private final IdEpoch idEpoch;
    private final boolean newResourceRequirement;
    private final Map<JobVertexID, VertexParallelismRescale> parallelisms;
    private final Map<SlotSharingGroupId, SlotSharingGroupRescale> slots;

    private final SchedulerStateHistory schedulerStates;

    private Long startTimestamp;
    private Long endTimestamp;
    private RescaleStatus status = RescaleStatus.Unknown;

    private TriggerCause triggerCause;
    private String sealedDescription;

    private @Nullable ErrorInfo errorInfo;

    Rescale(RescaleTimeline rescaleTimeline, Logger log, boolean newResourceRequirement) {
        this.idEpoch = rescaleTimeline.nextRescaleId(newResourceRequirement);
        this.newResourceRequirement = newResourceRequirement;
        this.parallelisms = new HashMap<>();
        this.slots = new HashMap<>();
        this.schedulerStates = new SchedulerStateHistory();
        this.timeline = rescaleTimeline;
        this.log = log;
    }

    public long getId() {
        return idEpoch.getRescaleId();
    }

    private Rescale addSchedulerState(SchedulerStateSpan schedulerStateSpan) {
        if (isSealed()) {
            log.warn(
                    "Rescale is already sealed. The scheduler state {} will be ignored.",
                    schedulerStateSpan);
            return this;
        }
        if (errorInfo != null) {
            schedulerStateSpan.setErrorInfo(errorInfo);
            errorInfo = null;
        }
        this.schedulerStates.addSchedulerStateSpan(schedulerStateSpan);
        return this;
    }

    public Rescale addSchedulerStateForced(State state) {
        return addSchedulerStateForced(state, null);
    }

    public Rescale addSchedulerStateForced(State schedulerState, @Nullable Throwable throwable) {
        long epochMilli = Instant.now().toEpochMilli();
        SchedulerStateSpan span;
        if (throwable != null) {
            span =
                    new SchedulerStateSpan(
                            schedulerState, new ErrorInfo(throwable, epochMilli), epochMilli);

        } else {
            span = new SchedulerStateSpan(schedulerState, epochMilli);
        }
        return addSchedulerState(span);
    }

    public TriggerCause getTriggerCause() {
        return triggerCause;
    }

    public void setErrorInfo(Throwable throwable) {
        this.errorInfo = new ErrorInfo(throwable, Instant.now().toEpochMilli());
    }

    public Rescale setStatus(Optional<RescaleStatus> statusOptional) {
        return setStatus(statusOptional.orElse(RescaleStatus.Unknown));
    }

    public Rescale setStatus(RescaleStatus status) {
        if (status == RescaleStatus.Unknown) {
            log.warn("Received unknown status from scheduler state.");
            return this;
        }
        Preconditions.checkState(!this.status.isSealed());
        this.status = Preconditions.checkNotNull(status);
        return this;
    }

    public Duration getDuration() {
        if (this.status.isSealed() && startTimestamp != null && endTimestamp != null) {
            return Duration.ofMillis(startTimestamp - endTimestamp);
        }
        return Duration.ZERO;
    }

    public Rescale setSealedDescription(String sealedDescription) {
        Preconditions.checkState(Objects.isNull(this.sealedDescription));
        this.sealedDescription = sealedDescription;
        return this;
    }

    public Rescale setStartTimestamp(long timestamp) {
        Preconditions.checkState(Objects.isNull(this.startTimestamp));
        this.startTimestamp = timestamp;
        return this;
    }

    public Rescale setEndTimestamp(Long endTimestamp) {
        Preconditions.checkState(Objects.isNull(this.endTimestamp));
        this.endTimestamp = endTimestamp;
        return this;
    }

    public RescaleStatus getStatus() {
        return status;
    }

    public Rescale setRequiredSlots() {
        JobInformation jobInformation = timeline.getJobInformation();
        for (SlotSharingGroup sharingGroup : jobInformation.getSlotSharingGroups()) {
            int requiredSlots =
                    sharingGroup.getJobVertexIds().stream()
                            .map(
                                    jobVertexID ->
                                            jobInformation
                                                    .getVertexInformation(jobVertexID)
                                                    .getParallelism())
                            .max(Integer::compare)
                            .orElseThrow();
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

    public Optional<Map<SlotSharingGroupId, Integer>> getAcquiredSlots() {
        if (slots.isEmpty()) {
            return Optional.empty();
        }
        Optional<SlotSharingGroupRescale> any = slots.values().stream().findAny();
        if (any.isEmpty() || any.get().getAcquiredSlots() == null) {
            return Optional.empty();
        }
        return Optional.of(
                slots.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry -> entry.getValue().getAcquiredSlots())));
    }

    public Optional<VertexParallelism> getAcquiredVertexParallelism() {
        if (parallelisms.isEmpty()) {
            return Optional.empty();
        }
        Optional<VertexParallelismRescale> any = parallelisms.values().stream().findAny();
        if (any.isEmpty() || any.get().getAcquiredParallelism() == null) {
            return Optional.empty();
        }
        return Optional.of(
                new VertexParallelism(
                        parallelisms.entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                                Map.Entry::getKey,
                                                kv -> kv.getValue().getAcquiredParallelism()))));
    }

    public Rescale setSufficientSlots() {
        JobInformation jobInformation = timeline.getJobInformation();
        final Map<SlotSharingGroupId, SlotSharingGroupMetaInfo> slotSharingGroupMetaInfo =
                SlotSharingGroupMetaInfo.from(jobInformation.getVertices());
        for (Map.Entry<SlotSharingGroupId, SlotSharingGroupMetaInfo> entry :
                slotSharingGroupMetaInfo.entrySet()) {
            SlotSharingGroupId groupId = entry.getKey();
            SlotSharingGroupRescale slotSharingGroupRescale =
                    slots.computeIfAbsent(groupId, SlotSharingGroupRescale::new);
            slotSharingGroupRescale.setSufficientSlots(entry.getValue().getMaxLowerBound());
        }
        return this;
    }

    public Rescale setCurrentSlotsAndParallelisms() {
        Rescale lastCompletedRescale = timeline.lastCompletedRescale();
        if (lastCompletedRescale == null) {
            log.warn("No available previous parallelism to set.");
            return this;
        }
        for (JobVertexID jobVertexID : parallelisms.keySet()) {
            Integer previousAcquiredParallelism =
                    lastCompletedRescale.parallelisms.get(jobVertexID).getAcquiredParallelism();
            VertexParallelismRescale vertexParallelismRescale =
                    parallelisms.computeIfAbsent(jobVertexID, VertexParallelismRescale::new);
            vertexParallelismRescale.setCurrentParallelism(previousAcquiredParallelism);
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

    public Rescale setRequiredVertexParallelism() {
        JobInformation jobInformation = timeline.getJobInformation();
        Map<JobVertexID, VertexParallelismInformation> allParallelismInfo =
                jobInformation.getVertexParallelismStore().getAllParallelismInfo();
        for (Map.Entry<JobVertexID, VertexParallelismInformation> entry :
                allParallelismInfo.entrySet()) {
            JobVertexID jvId = entry.getKey();
            VertexParallelismInformation vertexParallelInfo = entry.getValue();
            VertexParallelismRescale vertexParallelismRescale =
                    this.parallelisms.computeIfAbsent(
                            jvId, jobVertexID -> new VertexParallelismRescale(jvId));
            SlotSharingGroup slotSharingGroup =
                    jobInformation.getVertexInformation(jvId).getSlotSharingGroup();
            vertexParallelismRescale.setSlotSharingGroupMetaInfo(slotSharingGroup);
            vertexParallelismRescale.setVertexName(jobInformation.getVertexName(jvId));
            vertexParallelismRescale.setRequiredParallelismWithBounds(vertexParallelInfo);
        }
        return this;
    }

    public boolean isSealed() {
        return status.isSealed() && startTimestamp != null && endTimestamp != null;
    }

    public Rescale setAcquiredVertexParallelism(VertexParallelism acquiredVertexParallelism) {
        Set<JobVertexID> vertices = acquiredVertexParallelism.getVertices();
        for (JobVertexID vertexID : vertices) {
            VertexParallelismRescale vertexParallelismRescale = this.parallelisms.get(vertexID);
            vertexParallelismRescale.setAcquiredParallelism(
                    acquiredVertexParallelism.getParallelism(vertexID));
        }
        return this;
    }

    public Rescale setTriggerCause(TriggerCause triggerCause) {
        this.triggerCause = triggerCause;
        return this;
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
                + ", triggerTimestamp="
                + startTimestamp
                + ", endTimestamp="
                + endTimestamp
                + ", status="
                + status
                + ", triggerCause='"
                + triggerCause
                + '\''
                + ", sealedDescription='"
                + sealedDescription
                + '\''
                + '}';
    }
}
