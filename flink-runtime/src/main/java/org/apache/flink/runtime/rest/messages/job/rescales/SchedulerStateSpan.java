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

package org.apache.flink.runtime.rest.messages.job.rescales;

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** Utils class to record the information of a scheduler state. */
public class SchedulerStateSpan implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_STATE = "state";
    public static final String FIELD_NAME_DURATION_START = "duration_start";
    public static final String FIELD_NAME_DURATION_END = "duration_end";
    public static final String FIELD_NAME_DURATION = "duration";
    public static final String FIELD_NAME_EXCEPTION = "stringed_exception";

    @JsonProperty(FIELD_NAME_STATE)
    private final String state;

    @Nullable
    @JsonProperty(FIELD_NAME_DURATION_START)
    private final Long inTimestamp;

    @Nullable
    @JsonProperty(FIELD_NAME_DURATION_END)
    private final Long outTimestamp;

    @Nullable
    @JsonProperty(FIELD_NAME_DURATION)
    private final Long duration;

    @Nullable
    @JsonProperty(FIELD_NAME_EXCEPTION)
    private final String stringedException;

    @JsonCreator
    public SchedulerStateSpan(
            @JsonProperty(FIELD_NAME_STATE) String state,
            @JsonProperty(FIELD_NAME_DURATION_START) Long logicStartMillis,
            @JsonProperty(FIELD_NAME_DURATION_END) Long logicEndMillis,
            @JsonProperty(FIELD_NAME_DURATION) Long duration,
            @JsonProperty(FIELD_NAME_EXCEPTION) String stringedException) {
        this.state = Preconditions.checkNotNull(state);
        this.inTimestamp = logicStartMillis;
        this.outTimestamp = logicEndMillis;
        this.duration = duration;
        this.stringedException = stringedException;
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
                && Objects.equals(duration, that.duration)
                && Objects.equals(stringedException, that.stringedException);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, inTimestamp, outTimestamp, duration, stringedException);
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
                + ", duration="
                + duration
                + ", stringedException='"
                + stringedException
                + '\''
                + '}';
    }
}
