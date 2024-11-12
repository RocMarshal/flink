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

import org.apache.flink.runtime.util.BoundedFIFOQueue;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Objects;
import java.util.function.Consumer;

/** The class to store rescale history entries. */
public class RescaleHistory {

    @Nullable RescaleEvent current;
    private final BoundedFIFOQueue<RescaleEvent> rescaleEventHistory;

    public RescaleHistory(int maxSize) {
        this.rescaleEventHistory = new BoundedFIFOQueue<>(maxSize);
    }

    public void add(RescaleEvent entry) {
        rescaleEventHistory.add(entry);
    }

    public void tryUpdateCurrentEvent(@Nonnull Consumer<RescaleEvent> updater) {
        if (current != null) {
            updater.accept(current);
        }
    }

    public void setCurrent(@Nullable RescaleEvent current) {
        if (Objects.nonNull(current)) {
            Preconditions.checkState(RescaleStatus.isTerminated(current.getStatus()));
        }
        this.current = current;
    }

    public ArrayList<RescaleEvent> toArrayList() {
        return rescaleEventHistory.toArrayList();
    }

    @Override
    public String toString() {
        return "RescaleHistory{"
                + "current="
                + current
                + ", rescaleEventHistory="
                + rescaleEventHistory
                + '}';
    }
}
