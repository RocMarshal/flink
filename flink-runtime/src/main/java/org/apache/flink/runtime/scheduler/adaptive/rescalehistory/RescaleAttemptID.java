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

import org.apache.flink.util.AbstractID;

import java.util.Objects;

/** The class to represent the rescale attempt id in one resource requirements. */
public class RescaleAttemptID {
    final long rescaleId;
    final AbstractID resourceRequirementsEpochID;
    final int attempt;

    public RescaleAttemptID(
            long rescaleId, AbstractID resourceRequirementsEpochID, Integer attempt) {
        this.rescaleId = rescaleId;
        this.resourceRequirementsEpochID = resourceRequirementsEpochID;
        this.attempt = attempt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RescaleAttemptID that = (RescaleAttemptID) o;
        return Objects.equals(rescaleId, that.rescaleId)
                && Objects.equals(resourceRequirementsEpochID, that.resourceRequirementsEpochID)
                && Objects.equals(attempt, that.attempt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rescaleId, resourceRequirementsEpochID, attempt);
    }

    @Override
    public String toString() {
        return "RescaleAttemptID{"
                + "rescaleId="
                + rescaleId
                + ", resourceRequirementsEpochID="
                + resourceRequirementsEpochID
                + ", attempt="
                + attempt
                + '}';
    }
}
