/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.adaptive.rescalehistory.RescaleAttemptID;
import org.apache.flink.runtime.scheduler.adaptive.rescalehistory.RescaleEvent;
import org.apache.flink.runtime.scheduler.adaptive.rescalehistory.RescaleLine;
import org.apache.flink.runtime.scheduler.adaptive.rescalehistory.RescaleStatus;

/** {@link FailureResultUtil} contains helper methods for {@link FailureResult}. */
public class FailureResultUtil {
    public static <T extends StateTransitions.ToRestarting & StateTransitions.ToFailing>
            void restartOrFail(
                    FailureResult failureResult, T context, StateWithExecutionGraph sweg) {
        if (failureResult.canRestart()) {
            sweg.getLogger().info("Restarting job.", failureResult.getFailureCause());
            final RescaleLine rescaleLine = context.getRescaleLine();
            final RescaleAttemptID rescaleAttemptID = rescaleLine.nextRescaleAttemptID();
            final VertexParallelismStore requiredVertexParallelism =
                    rescaleLine.getRequiredVertexParallelism();
            rescaleLine
                    .tryUpdateCurrentEvent(
                            entry ->
                                    entry.setStatus(RescaleStatus.IGNORED)
                                            .setEndTimestamp(System.currentTimeMillis())
                                            .fillBackDuration()
                                            .setComment("Ignored by restarting."))
                    .resetCurrentEvent()
                    .addEventAsCurrent(
                            new RescaleEvent(rescaleAttemptID)
                                    .setTriggerTimestamp()
                                    .setStatus(RescaleStatus.TRYING)
                                    .setRequiredVertexParallelism(requiredVertexParallelism));
            context.goToRestarting(
                    sweg.getExecutionGraph(),
                    sweg.getExecutionGraphHandler(),
                    sweg.getOperatorCoordinatorHandler(),
                    failureResult.getBackoffTime(),
                    false,
                    sweg.getFailures());
        } else {
            sweg.getLogger().info("Failing job.", failureResult.getFailureCause());
            context.goToFailing(
                    sweg.getExecutionGraph(),
                    sweg.getExecutionGraphHandler(),
                    sweg.getOperatorCoordinatorHandler(),
                    failureResult.getFailureCause(),
                    sweg.getFailures());
        }
    }
}
