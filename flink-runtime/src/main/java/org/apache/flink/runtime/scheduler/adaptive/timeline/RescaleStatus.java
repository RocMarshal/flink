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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The status of the rescale event. */
public enum RescaleStatus {
    Unknown,

    /**
     * It's aligned with {@link org.apache.flink.runtime.scheduler.adaptive.CreatingExecutionGraph}.
     */
    CreatingExecutionGraph,

    /**
     * It's aligned with {@link org.apache.flink.runtime.scheduler.adaptive.WaitingForResources}.
     */
    WaitingForResources,

    /** It's aligned with {@link org.apache.flink.runtime.scheduler.adaptive.Executing}. */
    Executing,

    /** It's aligned with {@link org.apache.flink.runtime.scheduler.adaptive.Restarting}. */
    Restarting,

    /** It represents that the target rescale event reached a terminated status successfully. */
    Completed,

    /** It represents that the target rescale event reached a terminated status with some errors. */
    Failed,

    /**
     * It represents that the target rescale event reached a terminated status, which is force ended
     * by a new rescale trigger condition.
     */
    Ignored;

    public static final Logger LOG = LoggerFactory.getLogger(RescaleStatus.class);

    public boolean isSealed() {
        return this == Failed || this == Ignored || this == Completed;
    }
}
