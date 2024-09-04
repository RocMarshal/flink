/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * The interface defines the methods to get the resource utilization and loading of task executors.
 */
public interface TaskExecutorsLoadingUtilization {

    TaskExecutorsLoadingUtilization EMPTY =
            new TaskExecutorsLoadingUtilization() {
                @Override
                public Map<ResourceID, LoadingWeight> getTaskExecutorsLoadingWeight() {
                    return Collections.emptyMap();
                }

                @Override
                public Map<ResourceID, SlotsUtilization> getTaskExecutorsSlotsUtilization() {
                    return Collections.emptyMap();
                }
            };

    /**
     * Return the loading weight for per task executor.
     *
     * @return map of loading weight for per task executor.
     */
    Map<ResourceID, LoadingWeight> getTaskExecutorsLoadingWeight();

    /**
     * Return the slots utilization for per task executor.
     *
     * @return the slots utilization for per task executor.
     */
    Map<ResourceID, SlotsUtilization> getTaskExecutorsSlotsUtilization();

    /** Help class to represent the slots utilization info. */
    class SlotsUtilization {
        private final int total;
        private final int reserved;

        public SlotsUtilization(int total, int reserved) {
            Preconditions.checkArgument(total > 0);
            Preconditions.checkArgument(reserved > 0);
            Preconditions.checkArgument(total >= reserved);
            this.total = total;
            this.reserved = reserved;
        }

        public SlotsUtilization incReserved(int inc) {
            Preconditions.checkArgument(inc > 0);
            Preconditions.checkArgument(reserved + inc <= total);
            return new SlotsUtilization(total, reserved + inc);
        }

        public double getUtilization() {
            return (double) reserved / total;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SlotsUtilization that = (SlotsUtilization) o;
            return total == that.total && reserved == that.reserved;
        }

        @Override
        public int hashCode() {
            return Objects.hash(total, reserved);
        }
    }
}
