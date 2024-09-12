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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobmaster.SlotInfo;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;

/** The helper class to represent the allocation score on the specified group and allocated slot. */
public class AllocationScore implements Comparable<AllocationScore> {

    private final ScoreKey scoreKey;
    private final long score;

    public AllocationScore(String groupId, AllocationID allocationId, long score) {
        this.scoreKey = new ScoreKey(groupId, allocationId);
        this.score = score;
    }

    public ScoreKey getScoreKey() {
        return scoreKey;
    }

    public String getGroupId() {
        return scoreKey.groupId;
    }

    public AllocationID getAllocationId() {
        return scoreKey.allocationID;
    }

    public long getScore() {
        return score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AllocationScore allocationScore = (AllocationScore) o;
        return score == allocationScore.score && Objects.equals(scoreKey, allocationScore.scoreKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scoreKey, score);
    }

    @Override
    public int compareTo(AllocationScore other) {
        int result = Long.compare(score, other.score);
        if (result != 0) {
            return result;
        }
        result = other.getAllocationId().compareTo(scoreKey.allocationID);
        if (result != 0) {
            return result;
        }
        return other.getGroupId().compareTo(scoreKey.groupId);
    }

    public static int compares(@Nullable AllocationScore las, @Nullable AllocationScore ras) {
        if (Objects.equals(las, ras)) {
            return 0;
        }
        return Objects.isNull(las) ? -1 : Objects.isNull(ras) ? 1 : las.compareTo(ras);
    }

    public static ScoreKey toKey(ExecutionSlotSharingGroup group, SlotInfo slotInfo) {
        return new ScoreKey(group.getId(), slotInfo.getAllocationId());
    }

    /**
     * The helper class to represent the key of the allocation score on the specified group and
     * allocated slot.
     */
    public static class ScoreKey {
        private final String groupId;
        private final AllocationID allocationID;

        private ScoreKey(String groupId, AllocationID allocationID) {
            this.groupId = groupId;
            this.allocationID = allocationID;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ScoreKey that = (ScoreKey) o;
            return Objects.equals(groupId, that.groupId)
                    && Objects.equals(allocationID, that.allocationID);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupId, allocationID);
        }
    }
}
