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

import javax.annotation.Nonnull;

import java.util.function.Consumer;

/** The rescale line information updating interface. */
public interface RescaleLine {

    /** Try update the current rescale event if not null. */
    RescaleLine tryUpdateCurrentEvent(@Nonnull Consumer<RescaleEvent> updater);

    /** Reset the current rescale event as null. */
    RescaleLine resetCurrentEvent();

    /**
     * * Add a rescale event as the current rescale event of the rescale line.
     *
     * @param rescaleEvent The rescale event to add.
     */
    void addEventAsCurrent(RescaleEvent rescaleEvent);

    /**
     * * Get the next rescale attempt id.
     *
     * @return The next rescale attempt id.
     */
    RescaleAttemptID nextRescaleAttemptID();

    /**
     * Get the required vertices parallelism of the current rescale event.
     *
     * @return The required vertices parallelism of the current rescale event.
     */
    VertexParallelismStore getRequiredVertexParallelism();

    /**
     * * Judge the previous scheduler state if is the instance of the specified class.
     *
     * @param klass The expected previous class.
     * @return ture if the previous state is the instance of the class, false else.
     */
    boolean preSchedulerStateInstanceOf(Class<?> klass);
}
