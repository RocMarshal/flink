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

import javax.annotation.Nonnull;

/** The status of the rescale event. */
public enum RescaleStatus {
    /** Trying to rescale with the specified resources requirements. */
    TRYING,
    /** Waiting for resources of the specified resources requirements. */
    WAITING_RESOURCES,
    /** Try on assigning resources. */
    ASSIGNING_RESOURCES,
    /** Succeeded in the rescale. */
    SUCCEEDED,
    /** Ignored that is caused by the new resources requirements or unexpected status. */
    IGNORED,
    /** Any error occurs to the current rescaling. */
    FAILED;

    public static boolean isTerminated(@Nonnull RescaleStatus status) {
        return status == SUCCEEDED || status == FAILED || status == IGNORED;
    }
}
