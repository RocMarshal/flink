/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.SlotSharingGroupMetaInfo;

/** The utils class to compute the matching between requested resource and allocated resource. */
public class AdaptiveResourceComputer {

    final Map<SlotSharingGroup, SlotSharingGroupMetaInfo> slotSharingGroupsMetaInfo;
    final List<SlotInfo> allocatedSlots;

    public AdaptiveResourceComputer(
            Map<SlotSharingGroup, SlotSharingGroupMetaInfo> slotSharingGroupMetaInfo,
            List<SlotInfo> allocatedSlots) {
        this.slotSharingGroupsMetaInfo = Preconditions.checkNotNull(slotSharingGroupMetaInfo);
        this.allocatedSlots = Preconditions.checkNotNull(allocatedSlots);
    }

    public boolean isMinimalSufficient() {
        Map<SlotSharingGroup, Integer> requestMinFineGrained =
                getRequestedMetaInfo(rp->!rp.equals(ResourceProfile.UNKNOWN), SlotSharingGroupMetaInfo::getMaxLowerBound);
        Map<SlotSharingGroup, Integer> requestMinUnknown =
                getRequestedMetaInfo(rp->rp.equals(ResourceProfile.UNKNOWN), SlotSharingGroupMetaInfo::getMaxLowerBound);

        Map<ResourceProfile, Integer> allocatedProfiles = getAllocatedProfiles();

        for (Map.Entry<SlotSharingGroup, Integer> reqResourceCnt :
                requestMinFineGrained.entrySet()) {
            ResourceProfile reqResourceProfile = reqResourceCnt.getKey().getResourceProfile();
            int reqCnt = reqResourceCnt.getValue();
            int allocatedCnt = allocatedProfiles.getOrDefault(reqResourceProfile, 0);
            int remaining = allocatedCnt - reqCnt;
            if (remaining >= 0) {
                allocatedProfiles.put(reqResourceProfile, remaining);
                continue;
            }
            // Borrow from ANY cnt.
            allocatedProfiles.put(reqResourceProfile, remaining);
            int anyProfileCnt = allocatedProfiles.getOrDefault(ResourceProfile.ANY, 0);
            int anyProfileRemaining = anyProfileCnt + remaining;
            if (anyProfileRemaining < 0) {
                return false;
            }
            allocatedProfiles.put(reqResourceProfile, 0);
            allocatedProfiles.put(ResourceProfile.ANY, anyProfileRemaining);
        }

        int requestMinUnknownNum = requestMinUnknown.values().stream().reduce(0, Integer::sum);
        int remainingAvailableSlots = allocatedProfiles.values().stream().reduce(0, Integer::sum);

        return remainingAvailableSlots >= requestMinUnknownNum;
    }

    <T> Map<SlotSharingGroup, T> getRequestedMetaInfo(
            Predicate<ResourceProfile> predicate,
            Function<SlotSharingGroupMetaInfo, T> metaInfoMapper) {
        return slotSharingGroupsMetaInfo.entrySet().stream()
                .filter(
                        entry -> predicate.test(entry.getKey()
                                .getResourceProfile()))
                .collect(toMap(Map.Entry::getKey, entry -> metaInfoMapper.apply(entry.getValue())));
    }

    public Optional<Map<SlotSharingGroup, ParallelismInfo>> computeParallelism() {
        Map<SlotSharingGroup, ParallelismInfo> parallelismInfos = new HashMap<>();
        Map<SlotSharingGroup, SlotSharingGroupMetaInfo> requestedFineGrained =
                getRequestedMetaInfo(
                        rp -> !rp.equals(ResourceProfile.UNKNOWN),
                        metaInfo -> metaInfo);
        Map<SlotSharingGroup, SlotSharingGroupMetaInfo> requestMinUnknown =
                getRequestedMetaInfo(rp->rp.equals(ResourceProfile.UNKNOWN),  metaInfo -> metaInfo);
        Map<ResourceProfile, Integer> allocatedProfiles = getAllocatedProfiles();

        for (Map.Entry<SlotSharingGroup, SlotSharingGroupMetaInfo> reqResourceCnt :
                requestedFineGrained.entrySet()) {
            ResourceProfile reqResourceProfile = reqResourceCnt.getKey().getResourceProfile();
            SlotSharingGroupMetaInfo metaInfo = reqResourceCnt.getValue();
            int allocatedCnt = allocatedProfiles.getOrDefault(reqResourceProfile, 0);
            int remaining = allocatedCnt - metaInfo.getMaxLowerBound();
            if (remaining >= 0) {
                allocatedProfiles.put(reqResourceProfile, remaining);
                ParallelismInfo parallelismInfo = parallelismInfos.compute(
                        reqResourceCnt.getKey(),
                        (slotSharingGroup, oldP) ->
                                oldP == null ? new ParallelismInfo(metaInfo) : oldP);
                parallelismInfo.addMatch(reqResourceProfile, metaInfo.getMaxLowerBound());
                continue;
            }
            // Borrow from ANY cnt.
            allocatedProfiles.put(reqResourceProfile, remaining);
            int anyProfileCnt = allocatedProfiles.getOrDefault(ResourceProfile.ANY, 0);
            int anyProfileRemaining = anyProfileCnt + remaining;
            if (anyProfileRemaining < 0) {
                return Optional.empty();
            }
            allocatedProfiles.put(reqResourceProfile, 0);
            allocatedProfiles.put(ResourceProfile.ANY, anyProfileRemaining);
            ParallelismInfo parallelismInfo = parallelismInfos.compute(
                    reqResourceCnt.getKey(),
                    (slotSharingGroup, oldP) ->
                            oldP == null ? new ParallelismInfo(metaInfo) : oldP);
            parallelismInfo.addMatch(ResourceProfile.ANY, -remaining);
        }

        // TODO...
        for (Map.Entry<SlotSharingGroup, SlotSharingGroupMetaInfo> reqUnknownMeta : requestMinUnknown.entrySet()) {

        }
        return Optional.of(parallelismInfos);
    }

    private Map<ResourceProfile, Integer> getAllocatedProfiles() {
        return
                allocatedSlots.stream()
                        .collect(
                                Collectors.groupingBy(
                                        SlotInfo::getResourceProfile,
                                        Collectors.reducing(0, e -> 1, Integer::sum)));
    }

    /** Helper class. */
    public static class ParallelismInfo {
        SlotSharingGroupMetaInfo metaInfo;
        Map<ResourceProfile, Integer> candidateMatches;

        public ParallelismInfo(
                SlotSharingGroupMetaInfo metaInfo) {
            this.metaInfo = metaInfo;
            this.candidateMatches = new HashMap<>();
        }

        public ParallelismInfo addMatch(ResourceProfile slotProfile, Integer cnt) {
            this.candidateMatches.compute(
                    slotProfile,
                    (resourceProfile, oldCnt) -> oldCnt != null ? oldCnt + cnt : cnt);
            return ParallelismInfo.this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ParallelismInfo that = (ParallelismInfo) o;
            return Objects.equals(metaInfo, that.metaInfo)
                    && Objects.equals(candidateMatches, that.candidateMatches);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metaInfo, candidateMatches);
        }
    }
}
