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

/** The rescale line information updating interface. */
public interface RescaleTimeline {

    JobInformation getJobInformation();

    /**
     * Judge whether the current rescale in timeline is terminated or empty.
     *
     * @return <code>ture</code> if contain <code>false</code> else.
     */
    boolean inIdling();

    boolean inRescalingProgress();

    /**
     * Create a new rescale and assign it as current rescale.
     *
     * @param newRescaleEpoch It represents whether the rescale resource requirements is in the new
     *     epoch.
     * @return <code>ture</code> if create successfully <code>false</code> else.
     */
    boolean newCurrentRescale(boolean newRescaleEpoch);

    @Nullable
    Rescale latestRescale(TerminalState terminalState);

    /**
     * Update the current rescale.
     *
     * @param rescaleUpdater The action to update the current rescale.
     * @return <code>ture</code> if update successfully <code>false</code> else.
     */
    boolean updateCurrentRescale(RescaleUpdater rescaleUpdater);

    /** Rescale operation interface. */
    interface RescaleUpdater {
        void update(Rescale rescaleToUpdate);
    }

    RescalesStatsSnapshot createSnapshot();

    /** Default implementation of {@link RescaleTimeline} without any actions. */
    enum NoOpRescaleTimeline implements RescaleTimeline {
        INSTANCE;

        @Override
        public boolean newCurrentRescale(boolean newRescaleEpoch) {
            return false;
        }

        @Override
        public boolean updateCurrentRescale(RescaleUpdater rescaleUpdater) {
            return false;
        }

        @Nullable
        @Override
        public Rescale latestRescale(TerminalState terminalState) {
            return null;
        }

        @Override
        public JobInformation getJobInformation() {
            return null;
        }

        @Override
        public boolean inIdling() {
            return false;
        }

        @Override
        public boolean inRescalingProgress() {
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

        @Override
        public RescalesStatsSnapshot createSnapshot() {
            return RescalesStatsSnapshot.emptySnapshot();
        }
    }
}
