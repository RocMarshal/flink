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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** The rescale line information updating interface. */
public interface RescaleTimeline {

    IdEpoch nextRescaleId(boolean newRescaleEpoch);

    void tryRolloutCurrentPendingRescale(
            boolean newRescaleEpoch,
            @Nullable Consumer<Rescale> sealAction,
            @Nullable Consumer<Rescale> updateAction);

    void tryUpdateCurrentPendingRescale(Consumer<Rescale> updateAction);

    @Nullable
    Rescale lastCompletedRescale();

    @Nullable
    Rescale currentRescale();

    /** Get the job information. */
    JobInformation getJobInformation();

    List<Rescale> historyToList();

    /** Default implementation of {@link RescaleTimeline} without any actions. */
    enum NoOpRescaleTimeline implements RescaleTimeline {
        INSTANCE;

        @Override
        public IdEpoch nextRescaleId(boolean newRescaleEpoch) {
            return null;
        }

        @Override
        public void tryRolloutCurrentPendingRescale(
                boolean newRescaleEpoch,
                @Nullable Consumer<Rescale> sealAction,
                @Nullable Consumer<Rescale> updateAction) {}

        @Override
        public void tryUpdateCurrentPendingRescale(Consumer<Rescale> updateAction) {}

        @Nullable
        @Override
        public Rescale lastCompletedRescale() {
            return null;
        }

        @Nullable
        @Override
        public Rescale currentRescale() {
            return null;
        }

        @Override
        public JobInformation getJobInformation() {
            return null;
        }

        @Override
        public List<Rescale> historyToList() {
            return new ArrayList<>();
        }
    }
}
