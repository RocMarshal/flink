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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testtasks.OnceBlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.InternalMiniClusterExtension;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.RestartStrategyUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for recording rescale history by {@link DefaultRescaleTimeline} or {@link
 * RescaleTimeline.NoOpRescaleTimeline}.
 */
class RescaleRecordingLogicITCase {
    static final String DISABLED_DESCRIPTION =
            "TODO: Blocked by FLINK-38343, the ITCases need the SchedulerNG#requstJob() to get the rescale history.";

    private static final Logger LOG = LoggerFactory.getLogger(RescaleRecordingLogicITCase.class);

    private static final int NUMBER_SLOTS_PER_TASK_MANAGER = 2;
    private static final int NUMBER_TASK_MANAGERS = 2;
    private static final int PARALLELISM = NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS;
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();

    private final Configuration configuration = createConfiguration();

    private final InternalMiniClusterExtension internalMiniClusterExtension =
            new InternalMiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(configuration)
                            .setNumberSlotsPerTaskManager(NUMBER_SLOTS_PER_TASK_MANAGER)
                            .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
                            .build());

    @RegisterExtension
    private final EachCallbackWrapper<InternalMiniClusterExtension> eachCallbackWrapper =
            new EachCallbackWrapper<>(internalMiniClusterExtension);

    private Configuration createConfiguration() {
        final Configuration configuration = new Configuration();

        configuration.set(WebOptions.MAX_ADAPTIVE_SCHEDULER_RESCALE_HISTORY_SIZE, 2);

        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ofMillis(1L));
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING,
                Duration.ofMillis(1L));
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ofMillis(1L));
        // required for #testCheckpointStatsPersistedAcrossRescale
        configuration.set(WebOptions.CHECKPOINTS_HISTORY_SIZE, Integer.MAX_VALUE);

        return configuration;
    }

    @BeforeEach
    void setUp() {
        OnceBlockingNoOpInvokable.reset();
    }

    private JobGraph createBlockingJobGraph(int parallelism) {
        final JobVertex blockingOperator = new JobVertex("Blocking operator", JOB_VERTEX_ID);
        SlotSharingGroup sharingGroup = new SlotSharingGroup();
        sharingGroup.setSlotSharingGroupName("ssga");
        blockingOperator.setSlotSharingGroup(sharingGroup);

        blockingOperator.setInvokableClass(OnceBlockingNoOpInvokable.class);

        blockingOperator.setParallelism(parallelism);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(blockingOperator);

        RestartStrategyUtils.configureFixedDelayRestartStrategy(jobGraph, 1, 0L);

        return jobGraph;
    }

    private void waitUntilParallelismForVertexReached(
            JobID jobId, JobVertexID jobVertexId, int targetParallelism) throws Exception {

        CommonTestUtils.waitUntilCondition(
                () -> {
                    final ArchivedExecutionGraph archivedExecutionGraph =
                            internalMiniClusterExtension
                                    .getMiniCluster()
                                    .getArchivedExecutionGraph(jobId)
                                    .get();

                    final AccessExecutionJobVertex executionJobVertex =
                            archivedExecutionGraph.getAllVertices().get(jobVertexId);

                    if (executionJobVertex == null) {
                        // parallelism was not yet determined
                        return false;
                    }

                    return executionJobVertex.getParallelism() == targetParallelism;
                });
    }

    private void waitUntilJobRunning(
            JobID jobId, JobVertexID jobVertexId, int targetParallelism) throws Exception {

        CommonTestUtils.waitUntilCondition(
                () -> {
                    JobStatus jobStatus = internalMiniClusterExtension
                            .getMiniCluster().getJobStatus(jobId).get();
                    return jobStatus == JobStatus.RUNNING;
                });
    }

    // Tests for rescale trigger causes.
    @Test
    void testRecordingRescaleTriggerredByInitialSchedule() throws Exception {
        final MiniCluster miniCluster = internalMiniClusterExtension.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();


        waitUntilParallelismForVertexReached(
                jobGraph.getJobID(),
                JOB_VERTEX_ID,
                NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS);

        miniCluster.terminateTaskManager(0);

        waitUntilJobRunning(
                jobGraph.getJobID(),
                JOB_VERTEX_ID,
                NUMBER_SLOTS_PER_TASK_MANAGER * (NUMBER_TASK_MANAGERS - 1));
        OnceBlockingNoOpInvokable.unblock();

        final CompletableFuture<ExecutionGraphInfo> executionGraphInfoFuture =
                miniCluster.getExecutionGraphInfo(jobGraph.getJobID());
        final ExecutionGraphInfo executionGraphInfo = executionGraphInfoFuture.join();

        System.out.println(
                executionGraphInfo.getRescalesStatsSnapshot().getRescaleHistory().get(0));
        assertThat(executionGraphInfo.getRescalesStatsSnapshot());
    }

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTriggerredByUpdateRequirement() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTriggerredByNoResourceAvailable() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTriggerredByRecoverableFailover() {}

    // End of tests for rescale trigger causes.

    // Tests for rescale terminated reasons and terminal state.
    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByUnknownForcedRolling() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedBySucceeded() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByExceptionOccurred() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByResourceRequirementsUpdated() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByNoResourcesOrParallelismsChange() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByJobCancelling() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByJobFinished() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByJobFailing() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByJobRestarting() {}

    // End of tests for rescale terminated reasons and terminal state.

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testUseNonTerminatedRescaleToRecordMergingWithNewRecoverableFailureTriggerCause() {
        // Test for 'Merge the current non-terminated rescale and the new rescale triggered by
        // recoverable failover into the current rescale'.
        // anyone case.
    }

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingInProgressRescale() {
        // anyone case.
    }

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testDecoupledAndCorrectnessOfNoOpRescaleTimelineLogic() {}
}
