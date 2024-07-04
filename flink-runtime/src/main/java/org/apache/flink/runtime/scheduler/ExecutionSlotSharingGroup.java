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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/** Represents execution vertices that will run the same shared slot. */
@Internal
public class ExecutionSlotSharingGroup {

    private final Set<ExecutionVertexID> executionVertexIds;
    private final String id;

    private ResourceProfile resourceProfile = ResourceProfile.UNKNOWN;

    ExecutionSlotSharingGroup() {
        this.executionVertexIds = new HashSet<>();
        this.id = UUID.randomUUID().toString();
    }

    public ExecutionSlotSharingGroup(Set<ExecutionVertexID> executionVertexIds) {
        this.executionVertexIds = executionVertexIds;
        this.id = UUID.randomUUID().toString();
    }

    void addVertex(final ExecutionVertexID executionVertexId) {
        executionVertexIds.add(executionVertexId);
    }

    public String getId() {
        return id;
    }

    void setResourceProfile(ResourceProfile resourceProfile) {
        this.resourceProfile = Preconditions.checkNotNull(resourceProfile);
    }

    ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    Set<ExecutionVertexID> getExecutionVertexIds() {
        return Collections.unmodifiableSet(executionVertexIds);
    }

    public Set<ExecutionVertexID> getContainedExecutionVertices() {
        return Collections.unmodifiableSet(executionVertexIds);
    }

    @Override
    public String toString() {
        return "ExecutionSlotSharingGroup{"
                + "id="
                + id
                + ", executionVertexIds="
                + executionVertexIds
                + ", resourceProfile="
                + resourceProfile
                + '}';
    }
}
