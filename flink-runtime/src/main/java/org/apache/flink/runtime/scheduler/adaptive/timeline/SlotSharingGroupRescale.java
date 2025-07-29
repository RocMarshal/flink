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
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * The matching information of a requested {@link
 * org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup}.
 */
public class SlotSharingGroupRescale implements Serializable {

    private final SlotSharingGroupId slotSharingGroupId;
    private String slotSharingGroupName;
    private ResourceProfile requiredResourceProfile;
    private Integer desiredSlots;
    private Integer sufficientSlots;
    @Nullable private Integer currentSlots;
    @Nullable private Integer acquiredSlots;
    @Nullable private ResourceProfile acquiredResourceProfile;

    public SlotSharingGroupRescale(SlotSharingGroupId sharingGroupId) {
        this.slotSharingGroupId = Preconditions.checkNotNull(sharingGroupId);
    }

    public SlotSharingGroupId getSlotSharingGroupId() {
        return slotSharingGroupId;
    }

    public void setSlotSharingGroupMetaInfo(SlotSharingGroup slotSharingGroup) {
        this.slotSharingGroupName = slotSharingGroup.getSlotSharingGroupName();
        this.requiredResourceProfile = slotSharingGroup.getResourceProfile();
    }

    public String getSlotSharingGroupName() {
        return slotSharingGroupName;
    }

    public Integer getDesiredSlots() {
        return desiredSlots;
    }

    public void setDesiredSlots(Integer desiredSlots) {
        this.desiredSlots = desiredSlots;
    }

    public Integer getSufficientSlots() {
        return sufficientSlots;
    }

    public void setSufficientSlots(Integer sufficientSlots) {
        this.sufficientSlots = sufficientSlots;
    }

    @Nullable
    public Integer getCurrentSlots() {
        return currentSlots;
    }

    public void setCurrentSlots(Integer currentSlots) {
        this.currentSlots = currentSlots;
    }

    @Nullable
    public Integer getAcquiredSlots() {
        return acquiredSlots;
    }

    public void setAcquiredSlots(Integer acquiredSlots) {
        this.acquiredSlots = acquiredSlots;
    }

    public ResourceProfile getRequiredResourceProfile() {
        return requiredResourceProfile;
    }

    @Nullable
    public ResourceProfile getAcquiredResourceProfile() {
        return acquiredResourceProfile;
    }

    public void setAcquiredResourceProfile(ResourceProfile acquiredResourceProfile) {
        this.acquiredResourceProfile = acquiredResourceProfile;
    }

    @Override
    public String toString() {
        return "SlotSharingGroupRescale{"
                + "slotSharingGroupId='"
                + slotSharingGroupId
                + '\''
                + ", slotSharingGroupName='"
                + slotSharingGroupName
                + '\''
                + ", desiredSlots="
                + desiredSlots
                + ", sufficientSlots="
                + sufficientSlots
                + ", currentSlots="
                + currentSlots
                + ", acquiredSlots="
                + acquiredSlots
                + ", requestResourceProfile="
                + requiredResourceProfile
                + ", acquiredResourceProfile="
                + acquiredResourceProfile
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SlotSharingGroupRescale that = (SlotSharingGroupRescale) o;
        return Objects.equals(desiredSlots, that.desiredSlots)
                && Objects.equals(sufficientSlots, that.sufficientSlots)
                && Objects.equals(currentSlots, that.currentSlots)
                && Objects.equals(acquiredSlots, that.acquiredSlots)
                && Objects.equals(slotSharingGroupId, that.slotSharingGroupId)
                && Objects.equals(slotSharingGroupName, that.slotSharingGroupName)
                && Objects.equals(requiredResourceProfile, that.requiredResourceProfile)
                && Objects.equals(acquiredResourceProfile, that.acquiredResourceProfile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                slotSharingGroupId,
                slotSharingGroupName,
                desiredSlots,
                sufficientSlots,
                currentSlots,
                acquiredSlots,
                requiredResourceProfile,
                acquiredResourceProfile);
    }
}
