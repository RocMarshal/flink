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

/** The class that represents the ID of the resource requirements. */
public class RescaleIdGenerator {

    private long rescaleCount;

    private RescaleAttemptID rescaleAttemptID;

    public RescaleIdGenerator() {
        this.rescaleCount = 1;
        rescaleAttemptID = new RescaleAttemptID(this.rescaleCount, new AbstractID(), 0);
    }

    public RescaleAttemptID rollEpochAndGetRescaleAttemptID() {
        rescaleAttemptID = new RescaleAttemptID(++this.rescaleCount, new AbstractID(), 0);
        return rescaleAttemptID;
    }

    public RescaleAttemptID currentRescaleAttemptID() {
        return rescaleAttemptID;
    }

    public RescaleAttemptID nextRescaleAttemptID() {
        this.rescaleAttemptID =
                new RescaleAttemptID(
                        ++this.rescaleCount,
                        rescaleAttemptID.resourceRequirementsEpochID,
                        rescaleAttemptID.attempt + 1);
        return rescaleAttemptID;
    }
}
