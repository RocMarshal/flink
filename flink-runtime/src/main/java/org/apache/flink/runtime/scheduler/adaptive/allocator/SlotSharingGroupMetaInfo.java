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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.instance.SlotSharingGroupId;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class SlotSharingGroupMetaInfo {

    private final int minLowerBound;
    private final int maxLowerBound;
    private final int maxUpperBound;

    private SlotSharingGroupMetaInfo(int minLowerBound, int maxLowerBound, int maxUpperBound) {
        this.minLowerBound = minLowerBound;
        this.maxLowerBound = maxLowerBound;
        this.maxUpperBound = maxUpperBound;
    }

    public int getMinLowerBound() {
        return minLowerBound;
    }

    public int getMaxLowerBound() {
        return maxLowerBound;
    }

    public int getMaxUpperBound() {
        return maxUpperBound;
    }

    public int getMaxLowerUpperBoundRange() {
        return maxUpperBound - maxLowerBound;
    }

    public static Map<SlotSharingGroupId, SlotSharingGroupMetaInfo> from(
            Iterable<JobInformation.VertexInformation> vertices) {

        return getPerSlotSharingGroups(
                vertices,
                vertexInformation ->
                        new SlotSharingGroupMetaInfo(
                                vertexInformation.getMinParallelism(),
                                vertexInformation.getMinParallelism(),
                                vertexInformation.getParallelism()),
                (metaInfo1, metaInfo2) ->
                        new SlotSharingGroupMetaInfo(
                                Math.min(
                                        metaInfo1.getMinLowerBound(), metaInfo2.getMinLowerBound()),
                                Math.max(
                                        metaInfo1.getMaxLowerBound(), metaInfo2.getMaxLowerBound()),
                                Math.max(
                                        metaInfo1.getMaxUpperBound(),
                                        metaInfo2.getMaxUpperBound())));
    }

    private static <T> Map<SlotSharingGroupId, T> getPerSlotSharingGroups(
            Iterable<JobInformation.VertexInformation> vertices,
            Function<JobInformation.VertexInformation, T> mapper,
            BiFunction<T, T, T> reducer) {
        final Map<SlotSharingGroupId, T> extractedPerSlotSharingGroups = new HashMap<>();
        for (JobInformation.VertexInformation vertex : vertices) {
            extractedPerSlotSharingGroups.compute(
                    vertex.getSlotSharingGroup().getSlotSharingGroupId(),
                    (slotSharingGroupId, currentData) ->
                            currentData == null
                                    ? mapper.apply(vertex)
                                    : reducer.apply(currentData, mapper.apply(vertex)));
        }
        return extractedPerSlotSharingGroups;
    }
}
