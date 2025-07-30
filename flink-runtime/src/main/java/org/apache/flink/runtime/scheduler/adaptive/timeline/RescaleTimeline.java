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

import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation;

import javax.annotation.Nullable;

import java.util.function.Consumer;

/** The rescale line information updating interface. */
public interface RescaleTimeline {

    /**
     * Judge whether the timeline contain terminated rescale or empty rescale.
     *
     * @return <code>ture</code> if contain <code>false</code> else.
     */
    boolean inIdling();

    /**
     * Judge whether the timeline contain non-pending rescale.
     *
     * @return <code>ture</code> if contain <code>false</code> else.
     */
    boolean inPending();

    /**
     * Get the current rescale in the rescale timeline.
     *
     * @return The current rescale.
     */
    @Nullable
    Rescale currentRescale();

    /**
     * Create a new rescale and assign it as current rescale.
     *
     * @param newRescaleEpoch It represents whether the rescale resource requirements is in the new
     *     epoch.
     * @return <code>ture</code> if create successfully <code>false</code> else.
     */
    boolean newCurrentRescale(boolean newRescaleEpoch);

    /**
     * Get the latest rescale for the specified status.
     *
     * @param status The target rescale status.
     * @return The latest rescale for the specified status.
     */
    @Nullable
    Rescale latestRescale(RescaleStatus status);

    /**
     * Update the current rescale.
     *
     * @param updateAction The action to update the current rescale.
     * @return <code>ture</code> if update successfully <code>false</code> else.
     */
    boolean updateCurrentRescale(Consumer<Rescale> updateAction);

    /** Get the job information. */
    JobInformation getJobInformation();

    /** Default implementation of {@link RescaleTimeline} without any actions. */
    enum NoOpRescaleTimeline implements RescaleTimeline {
        INSTANCE;

        @Override
        public boolean newCurrentRescale(boolean newRescaleEpoch) {
            return false;
        }

        @Override
        public boolean updateCurrentRescale(Consumer<Rescale> updateAction) {
            return false;
        }

        @Override
        public Rescale latestRescale(RescaleStatus status) {
            return null;
        }

        @Override
        public boolean inIdling() {
            return false;
        }

        @Override
        public boolean inPending() {
            return false;
        }

        @Override
        public Rescale currentRescale() {
            return null;
        }

        @Override
        public JobInformation getJobInformation() {
            return null;
        }
    }
}
