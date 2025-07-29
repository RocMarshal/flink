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
    private final AbstractID rescaleUid;
    private final AbstractID resourceRequirementsEpoch;
    private final long subRescaleIdOfCurrentEpoch;

    public IdEpoch(
            long rescaleId, AbstractID resourceRequirementsEpoch, Long subRescaleIdOfCurrentEpoch) {
        this.rescaleId = rescaleId;
        this.resourceRequirementsEpoch = resourceRequirementsEpoch;
        this.subRescaleIdOfCurrentEpoch = subRescaleIdOfCurrentEpoch;
        this.rescaleUid = new AbstractID();
    }

    public AbstractID getRescaleUid() {
        return rescaleUid;
    }

    public long getRescaleId() {
        return rescaleId;
    }

    public AbstractID getResourceRequirementsEpoch() {
        return resourceRequirementsEpoch;
    }

    public long getSubRescaleIdOfCurrentEpoch() {
        return subRescaleIdOfCurrentEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IdEpoch idEpoch = (IdEpoch) o;
        return rescaleId == idEpoch.rescaleId
                && subRescaleIdOfCurrentEpoch == idEpoch.subRescaleIdOfCurrentEpoch
                && Objects.equals(rescaleUid, idEpoch.rescaleUid)
                && Objects.equals(resourceRequirementsEpoch, idEpoch.resourceRequirementsEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rescaleId, rescaleUid, resourceRequirementsEpoch, subRescaleIdOfCurrentEpoch);
    }

    @Override
    public String toString() {
        return "IdEpoch{"
                + "rescaleId="
                + rescaleId
                + ", rescaleUid="
                + rescaleUid
                + ", resourceRequirementsEpoch="
                + resourceRequirementsEpoch
                + ", subRescaleIdOfCurrentEpoch="
                + subRescaleIdOfCurrentEpoch
                + '}';
    }
}
