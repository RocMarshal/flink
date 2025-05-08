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

import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.scheduler.adaptive.State;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;

/** Utils class to record the information of a scheduler state. */
public class SchedulerStateSpan {

    private final String state;
    private @Nullable final Long inTimestamp;
    private @Nullable final Long outTimestamp;
    private @Nullable ErrorInfo exception;

    public SchedulerStateSpan(State state) {
        this.state = Preconditions.checkNotNull(state.getClass().getSimpleName());
        this.inTimestamp = state.getDurable().getInTimestamp();
        this.outTimestamp = state.getDurable().getOutTimestamp();
    }

    public SchedulerStateSpan(State state, long forceLogicEndMillis) {
        this(state, null, forceLogicEndMillis);
    }

    public SchedulerStateSpan(State state, ErrorInfo errorInfo, long forceLogicEndMillis) {
        this.state = Preconditions.checkNotNull(state.getClass().getSimpleName());
        this.inTimestamp = state.getDurable().getInTimestamp();
        this.outTimestamp = forceLogicEndMillis;
        this.exception = errorInfo;
    }

    public String getState() {
        return state;
    }

    public Long getInTimestamp() {
        return inTimestamp;
    }

    public Long getOutTimestamp() {
        return outTimestamp;
    }

    public boolean isSealed() {
        return inTimestamp != null && outTimestamp != null;
    }

    public Duration getDuration() {
        if (inTimestamp != null && outTimestamp != null) {
            return Duration.ofMillis(outTimestamp - inTimestamp);
        }
        return Duration.ZERO;
    }

    public void setErrorInfo(ErrorInfo errorInfo) {
        this.exception = errorInfo;
    }

    public @Nullable ErrorInfo getException() {
        return exception;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchedulerStateSpan that = (SchedulerStateSpan) o;
        return Objects.equals(state, that.state)
                && Objects.equals(inTimestamp, that.inTimestamp)
                && Objects.equals(outTimestamp, that.outTimestamp)
                && Objects.equals(exception, that.exception);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, inTimestamp, outTimestamp, exception);
    }

    @Override
    public String toString() {
        return "SchedulerStateSpan{"
                + "state='"
                + state
                + '\''
                + ", inTimestamp="
                + inTimestamp
                + ", outTimestamp="
                + outTimestamp
                + ", exceptions="
                + exception
                + '}';
    }
}
