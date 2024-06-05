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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Objects;

/** A class to represent the description of preferred resource. */
public final class PreferredResourceProfile {
    private final @Nonnull LoadingWeight preferredLoading;
    private final @Nonnull ResourceID taskExecutorId;

    public PreferredResourceProfile(LoadingWeight preferredLoading, ResourceID taskExecutorId) {
        this.preferredLoading = Preconditions.checkNotNull(preferredLoading);
        this.taskExecutorId = Preconditions.checkNotNull(taskExecutorId);
    }

    public LoadingWeight getPreferredLoading() {
        return preferredLoading;
    }

    public ResourceID getTaskExecutorId() {
        return taskExecutorId;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PreferredResourceProfile that = (PreferredResourceProfile) object;
        return Objects.equals(preferredLoading, that.preferredLoading)
                && Objects.equals(taskExecutorId, that.taskExecutorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(preferredLoading, taskExecutorId);
    }
}
