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

/** The cause of trigger rescaling. */
public enum TriggerCause {
    /** The first scheduling when the job starting. */
    INITIAL_SCHEDULE,
    /** Update job resource requirements. */
    UPDATE_REQUIREMENT,
    /** New resources available. */
    NEW_RESOURCE_AVAILABLE,
    // TODO: When here's in_progress rescale not completed, and a recoverable failover occurs here,
    // the rescale should be squished in one and the trigger cause will be set as
    // RECOVERABLE_FAILOVER.
    /** Recoverable failover. */
    RECOVERABLE_FAILOVER
}
