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

import org.apache.flink.util.AbstractID;

import java.io.Serializable;
import java.util.Objects;

/** The class to represent the rescale id description in one resource requirements epoch. */
public class IdEpoch implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long rescaleId;
    private final AbstractID resourceRequirementsEpoch;
    private final int subRescaleIdOfCurrentEpoch;

    public IdEpoch(
            long rescaleId,
            AbstractID resourceRequirementsEpoch,
            Integer subRescaleIdOfCurrentEpoch) {
        this.rescaleId = rescaleId;
        this.resourceRequirementsEpoch = resourceRequirementsEpoch;
        this.subRescaleIdOfCurrentEpoch = subRescaleIdOfCurrentEpoch;
    }

    public long getRescaleId() {
        return rescaleId;
    }

    public AbstractID getResourceRequirementsEpoch() {
        return resourceRequirementsEpoch;
    }

    public int getSubRescaleIdOfCurrentEpoch() {
        return subRescaleIdOfCurrentEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IdEpoch that = (IdEpoch) o;
        return Objects.equals(rescaleId, that.rescaleId)
                && Objects.equals(resourceRequirementsEpoch, that.resourceRequirementsEpoch)
                && Objects.equals(subRescaleIdOfCurrentEpoch, that.subRescaleIdOfCurrentEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rescaleId, resourceRequirementsEpoch, subRescaleIdOfCurrentEpoch);
    }

    @Override
    public String toString() {
        return "RescaleAttemptID{"
                + "rescaleId="
                + rescaleId
                + ", resourceRequirementsEpoch="
                + resourceRequirementsEpoch
                + ", subRescaleIdOfCurrentEpoch="
                + subRescaleIdOfCurrentEpoch
                + '}';
    }
}
