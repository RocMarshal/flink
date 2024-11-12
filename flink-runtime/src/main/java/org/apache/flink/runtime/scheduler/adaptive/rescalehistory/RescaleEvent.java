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

package org.apache.flink.runtime.scheduler.adaptive.rescalehistory;

import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/** Rescale event. */
public class RescaleEvent implements Serializable {

    private final long id;
    private final RescaleAttemptID rescaleAttemptID;
    private Long triggerTimestamp;
    private RescaleStatus status;
    private VertexParallelismStore requiredVertexParallelism;
    private @Nullable VertexParallelism acquiredVertexParallelism;
    private Map<String, Integer> requiredSlots;
    private Map<String, Integer> acquiredSlots;
    private Long endTimestamp;
    private Duration duration;
    private @Nullable String comment;

    public RescaleEvent(RescaleAttemptID rescaleAttemptID) {
        this.rescaleAttemptID = rescaleAttemptID;
        this.id = rescaleAttemptID.rescaleId;
    }

    public long getId() {
        return id;
    }

    public RescaleEvent setStatus(RescaleStatus status) {
        this.status = Preconditions.checkNotNull(status);
        return this;
    }

    public RescaleAttemptID getRescaleAttemptID() {
        return rescaleAttemptID;
    }

    public RescaleEvent setComment(String comment) {
        Preconditions.checkState(Objects.isNull(this.comment));
        this.comment = comment;
        return this;
    }

    public RescaleEvent setTriggerTimestamp() {
        Preconditions.checkState(Objects.isNull(this.triggerTimestamp));
        this.triggerTimestamp = System.currentTimeMillis();
        return this;
    }

    public RescaleEvent setEndTimestamp(Long endTimestamp) {
        Preconditions.checkState(Objects.isNull(this.endTimestamp));
        this.endTimestamp = endTimestamp;
        return this;
    }

    public Long getTriggerTimestamp() {
        return triggerTimestamp;
    }

    public VertexParallelismStore getRequiredVertexParallelism() {
        return requiredVertexParallelism;
    }

    public RescaleStatus getStatus() {
        return status;
    }

    public RescaleEvent fillBackDuration() {
        Preconditions.checkNotNull(triggerTimestamp);
        Preconditions.checkNotNull(endTimestamp);
        Preconditions.checkState(Objects.isNull(this.duration));
        this.duration = Duration.ofMillis(endTimestamp - triggerTimestamp);
        return this;
    }

    public RescaleEvent setRequiredVertexParallelism(
            VertexParallelismStore requiredVertexParallelism) {
        Preconditions.checkState(Objects.isNull(this.requiredVertexParallelism));
        this.requiredVertexParallelism = requiredVertexParallelism;
        return this;
    }

    public void setAcquiredVertexParallelism(VertexParallelism acquiredVertexParallelism) {
        Preconditions.checkState(Objects.isNull(this.acquiredVertexParallelism));
        this.acquiredVertexParallelism = acquiredVertexParallelism;
    }

    @Override
    public String toString() {
        return "RescaleEvent{"
                + "rescaleAttemptID="
                + rescaleAttemptID
                + ", status="
                + status
                + ", comment='"
                + comment
                + '\''
                + ", triggerTimestamp="
                + triggerTimestamp
                + ", endTimestamp="
                + endTimestamp
                + ", duration="
                + duration
                + ", vertexParallelismStore="
                + requiredVertexParallelism
                + ", acquiredVertexParallelism="
                + acquiredVertexParallelism
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RescaleEvent that = (RescaleEvent) o;
        return Objects.equals(rescaleAttemptID, that.rescaleAttemptID)
                && status == that.status
                && Objects.equals(comment, that.comment)
                && Objects.equals(triggerTimestamp, that.triggerTimestamp)
                && Objects.equals(endTimestamp, that.endTimestamp)
                && Objects.equals(duration, that.duration)
                && Objects.equals(requiredVertexParallelism, that.requiredVertexParallelism)
                && Objects.equals(acquiredVertexParallelism, that.acquiredVertexParallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rescaleAttemptID,
                status,
                comment,
                triggerTimestamp,
                endTimestamp,
                duration,
                requiredVertexParallelism,
                acquiredVertexParallelism);
    }
}
