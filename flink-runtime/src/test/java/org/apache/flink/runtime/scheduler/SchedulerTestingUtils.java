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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.executiongraph.failover.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.function.RunnableWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.finishJobVertex;
import static org.apache.flink.runtime.executiongraph.utils.ExecutionUtils.waitForTaskDeploymentDescriptorsCreation;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.apache.flink.runtime.util.JobVertexConnectionUtils.connectNewDataSetAsInput;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** A utility class to create {@link DefaultScheduler} instances for testing. */
public class SchedulerTestingUtils {

    private static final long DEFAULT_CHECKPOINT_TIMEOUT_MS = 10 * 60 * 1000;
    private static final long RETRY_INTERVAL_MILLIS = 10L;
    private static final int RETRY_ATTEMPTS = 6000;

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(300);

    private SchedulerTestingUtils() {}

    public static DefaultScheduler createScheduler(
            final JobGraph jobGraph,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final ScheduledExecutorService executorService)
            throws Exception {
        return new DefaultSchedulerBuilder(jobGraph, mainThreadExecutor, executorService).build();
    }

    public static void enableCheckpointing(final JobGraph jobGraph) {
        enableCheckpointing(jobGraph, null, null);
    }

    public static void enableCheckpointing(
            final JobGraph jobGraph,
            @Nullable StateBackend stateBackend,
            @Nullable CheckpointStorage checkpointStorage) {
        enableCheckpointing(
                jobGraph,
                stateBackend,
                checkpointStorage,
                Long.MAX_VALUE, // disable periodical checkpointing
                false);
    }

    public static void enableCheckpointing(
            final JobGraph jobGraph,
            @Nullable StateBackend stateBackend,
            @Nullable CheckpointStorage checkpointStorage,
            long checkpointInterval,
            boolean enableCheckpointsAfterTasksFinish) {

        final CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(checkpointInterval)
                        .setCheckpointTimeout(DEFAULT_CHECKPOINT_TIMEOUT_MS)
                        .setMinPauseBetweenCheckpoints(0)
                        .setMaxConcurrentCheckpoints(1)
                        .setCheckpointRetentionPolicy(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION)
                        .setExactlyOnce(false)
                        .setUnalignedCheckpointsEnabled(false)
                        .setTolerableCheckpointFailureNumber(0)
                        .setCheckpointIdOfIgnoredInFlightData(0)
                        .setEnableCheckpointsAfterTasksFinish(enableCheckpointsAfterTasksFinish)
                        .build();

        SerializedValue<StateBackend> serializedStateBackend = null;
        if (stateBackend != null) {
            try {
                serializedStateBackend = new SerializedValue<>(stateBackend);
            } catch (IOException e) {
                throw new RuntimeException("could not serialize state backend", e);
            }
        }

        SerializedValue<CheckpointStorage> serializedCheckpointStorage = null;
        if (checkpointStorage != null) {
            try {
                serializedCheckpointStorage = new SerializedValue<>(checkpointStorage);
            } catch (IOException e) {
                throw new RuntimeException("could not serialize checkpoint storage", e);
            }
        }

        jobGraph.setSnapshotSettings(
                new JobCheckpointingSettings(
                        config,
                        serializedStateBackend,
                        TernaryBoolean.UNDEFINED,
                        serializedCheckpointStorage,
                        null));
    }

    public static Collection<ExecutionAttemptID> getAllCurrentExecutionAttempts(
            SchedulerNG scheduler) {
        return StreamSupport.stream(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices()
                                .spliterator(),
                        false)
                .map((vertex) -> vertex.getCurrentExecutionAttempt().getAttemptId())
                .collect(Collectors.toList());
    }

    public static ExecutionState getExecutionState(
            DefaultScheduler scheduler, JobVertexID jvid, int subtask) {
        final ExecutionJobVertex ejv = getJobVertex(scheduler, jvid);
        return ejv.getTaskVertices()[subtask].getCurrentExecutionAttempt().getState();
    }

    public static void failExecution(DefaultScheduler scheduler, JobVertexID jvid, int subtask) {
        final ExecutionAttemptID attemptID = getAttemptId(scheduler, jvid, subtask);
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        attemptID, ExecutionState.FAILED, new Exception("test task failure")));
    }

    public static void canceledExecution(
            DefaultScheduler scheduler, JobVertexID jvid, int subtask) {
        final ExecutionAttemptID attemptID = getAttemptId(scheduler, jvid, subtask);
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        attemptID, ExecutionState.CANCELED, new Exception("test task failure")));
    }

    public static void setExecutionToState(
            ExecutionState executionState,
            DefaultScheduler scheduler,
            JobVertexID jvid,
            int subtask) {
        final ExecutionAttemptID attemptID = getAttemptId(scheduler, jvid, subtask);
        scheduler.updateTaskExecutionState(new TaskExecutionState(attemptID, executionState));
    }

    public static void setAllExecutionsToRunning(final SchedulerNG scheduler) {
        getAllCurrentExecutionAttempts(scheduler)
                .forEach(
                        (attemptId) -> {
                            scheduler.updateTaskExecutionState(
                                    new TaskExecutionState(attemptId, ExecutionState.INITIALIZING));
                            scheduler.updateTaskExecutionState(
                                    new TaskExecutionState(attemptId, ExecutionState.RUNNING));
                        });
    }

    public static void setAllExecutionsToCancelled(final DefaultScheduler scheduler) {
        for (final ExecutionAttemptID attemptId : getAllCurrentExecutionAttempts(scheduler)) {
            final boolean setToRunning =
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionState(attemptId, ExecutionState.CANCELED));

            assertThat(setToRunning).as("could not switch task to RUNNING").isTrue();
        }
    }

    public static void acknowledgePendingCheckpoint(
            final DefaultScheduler scheduler, final long checkpointId) throws CheckpointException {
        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);
        final JobID jid = scheduler.getJobId();

        for (ExecutionAttemptID attemptId : getAllCurrentExecutionAttempts(scheduler)) {
            final AcknowledgeCheckpoint acknowledgeCheckpoint =
                    new AcknowledgeCheckpoint(jid, attemptId, checkpointId);
            checkpointCoordinator.receiveAcknowledgeMessage(
                    acknowledgeCheckpoint, "Unknown location");
        }
    }

    public static void acknowledgePendingCheckpoint(
            final SchedulerNG scheduler,
            final int checkpointId,
            final Map<OperatorID, OperatorSubtaskState> subtaskStateMap) {
        getAllCurrentExecutionAttempts(scheduler)
                .forEach(
                        (executionAttemptID) -> {
                            scheduler.acknowledgeCheckpoint(
                                    scheduler.requestJob().getJobId(),
                                    executionAttemptID,
                                    checkpointId,
                                    new CheckpointMetrics(),
                                    new TaskStateSnapshot(subtaskStateMap));
                        });
    }

    public static CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
            DefaultScheduler scheduler) throws Exception {
        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);
        return checkpointCoordinator.triggerCheckpoint(false);
    }

    public static void acknowledgeCurrentCheckpoint(DefaultScheduler scheduler) {
        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints())
                .as("Coordinator has not ")
                .isOne();

        final PendingCheckpoint pc =
                checkpointCoordinator.getPendingCheckpoints().values().iterator().next();

        // because of races against the async thread in the coordinator, we need to wait here until
        // the
        // coordinator state is acknowledged. This can be removed once the CheckpointCoordinator is
        // executes all actions in the Scheduler's main thread executor.
        while (pc.getNumberOfNonAcknowledgedOperatorCoordinators() > 0) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }

        getAllCurrentExecutionAttempts(scheduler)
                .forEach(
                        (attemptId) ->
                                scheduler.acknowledgeCheckpoint(
                                        pc.getJobId(),
                                        attemptId,
                                        pc.getCheckpointID(),
                                        new CheckpointMetrics(),
                                        null));
    }

    @SuppressWarnings("deprecation")
    public static CheckpointCoordinator getCheckpointCoordinator(SchedulerBase scheduler) {
        return scheduler.getCheckpointCoordinator();
    }

    public static void waitForJobStatusRunning(final SchedulerNG scheduler) throws Exception {
        waitUntilCondition(
                () -> scheduler.requestJobStatus() == JobStatus.RUNNING,
                RETRY_INTERVAL_MILLIS,
                RETRY_ATTEMPTS);
    }

    public static void waitForCheckpointInProgress(final SchedulerNG scheduler) throws Exception {
        waitUntilCondition(
                () ->
                        scheduler
                                        .requestCheckpointStats()
                                        .getCounts()
                                        .getNumberOfInProgressCheckpoints()
                                > 0,
                RETRY_INTERVAL_MILLIS,
                RETRY_ATTEMPTS);
    }

    public static void waitForCompletedCheckpoint(final SchedulerNG scheduler) throws Exception {
        waitUntilCondition(
                () ->
                        scheduler
                                        .requestCheckpointStats()
                                        .getCounts()
                                        .getNumberOfCompletedCheckpoints()
                                > 0,
                RETRY_INTERVAL_MILLIS,
                RETRY_ATTEMPTS);
    }

    private static ExecutionJobVertex getJobVertex(
            DefaultScheduler scheduler, JobVertexID jobVertexId) {
        final ExecutionVertexID id = new ExecutionVertexID(jobVertexId, 0);
        return scheduler.getExecutionVertex(id).getJobVertex();
    }

    public static ExecutionAttemptID getAttemptId(
            DefaultScheduler scheduler, JobVertexID jvid, int subtask) {
        final ExecutionJobVertex ejv = getJobVertex(scheduler, jvid);
        assert ejv != null;
        return ejv.getTaskVertices()[subtask].getCurrentExecutionAttempt().getAttemptId();
    }

    // ------------------------------------------------------------------------

    public static SlotSharingExecutionSlotAllocatorFactory
            newSlotSharingExecutionSlotAllocatorFactory() {
        return newSlotSharingExecutionSlotAllocatorFactory(
                TestingPhysicalSlotProvider.createWithInfiniteSlotCreation());
    }

    public static SlotSharingExecutionSlotAllocatorFactory
            newSlotSharingExecutionSlotAllocatorFactory(PhysicalSlotProvider physicalSlotProvider) {
        return newSlotSharingExecutionSlotAllocatorFactory(physicalSlotProvider, DEFAULT_TIMEOUT);
    }

    public static SlotSharingExecutionSlotAllocatorFactory
            newSlotSharingExecutionSlotAllocatorFactory(
                    PhysicalSlotProvider physicalSlotProvider, Duration allocationTimeout) {
        return new SlotSharingExecutionSlotAllocatorFactory(
                physicalSlotProvider,
                true,
                new TestingPhysicalSlotRequestBulkChecker(),
                allocationTimeout,
                new LocalInputPreferredSlotSharingStrategy.Factory());
    }

    public static SchedulerBase createSchedulerAndDeploy(
            boolean isAdaptive,
            JobID jobId,
            JobVertex producer,
            JobVertex[] consumers,
            DistributionPattern distributionPattern,
            BlobWriter blobWriter,
            ComponentMainThreadExecutor mainThreadExecutor,
            ScheduledExecutorService ioExecutor,
            JobMasterPartitionTracker partitionTracker,
            ScheduledExecutorService scheduledExecutor,
            Configuration jobMasterConfiguration)
            throws Exception {
        final List<JobVertex> vertices = new ArrayList<>(Collections.singletonList(producer));
        IntermediateDataSetID dataSetId = new IntermediateDataSetID();
        for (JobVertex consumer : consumers) {
            connectNewDataSetAsInput(
                    consumer,
                    producer,
                    distributionPattern,
                    ResultPartitionType.BLOCKING,
                    dataSetId,
                    false);
            vertices.add(consumer);
        }

        final SchedulerBase scheduler =
                createScheduler(
                        isAdaptive,
                        jobId,
                        vertices,
                        blobWriter,
                        mainThreadExecutor,
                        ioExecutor,
                        partitionTracker,
                        scheduledExecutor,
                        jobMasterConfiguration);
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();
        final TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

        runUnsafe(
                () -> {
                    initializeExecutionJobVertex(producer.getID(), executionGraph, isAdaptive);
                    deployTasks(executionGraph, producer.getID(), slotBuilder);
                },
                mainThreadExecutor);

        waitForTaskDeploymentDescriptorsCreation(
                Objects.requireNonNull(executionGraph.getJobVertex(producer.getID()))
                        .getTaskVertices());

        // Transition upstream vertices into FINISHED
        runUnsafe(() -> finishJobVertex(executionGraph, producer.getID()), mainThreadExecutor);

        // Deploy downstream sink vertices
        for (JobVertex consumer : consumers) {
            runUnsafe(
                    () -> {
                        initializeExecutionJobVertex(consumer.getID(), executionGraph, isAdaptive);
                        deployTasks(executionGraph, consumer.getID(), slotBuilder);
                    },
                    mainThreadExecutor);
            waitForTaskDeploymentDescriptorsCreation(
                    Objects.requireNonNull(executionGraph.getJobVertex(consumer.getID()))
                            .getTaskVertices());
        }

        return scheduler;
    }

    private static void initializeExecutionJobVertex(
            JobVertexID jobVertex,
            ExecutionGraph executionGraph,
            final boolean adaptiveSchedulerEnabled) {
        if (!adaptiveSchedulerEnabled) {
            // This method call only needed for adaptive scheduler, no-op otherwise.
            return;
        }
        try {
            executionGraph.initializeJobVertex(
                    executionGraph.getJobVertex(jobVertex), System.currentTimeMillis());
            executionGraph.notifyNewlyInitializedJobVertices(
                    Collections.singletonList(executionGraph.getJobVertex(jobVertex)));
        } catch (JobException exception) {
            throw new RuntimeException(exception);
        }
    }

    private static DefaultScheduler createScheduler(
            boolean isAdaptive,
            JobID jobId,
            List<JobVertex> jobVertices,
            BlobWriter blobWriter,
            ComponentMainThreadExecutor mainThreadExecutor,
            ScheduledExecutorService ioExecutor,
            JobMasterPartitionTracker partitionTracker,
            ScheduledExecutorService scheduledExecutor,
            Configuration jobMasterConfiguration)
            throws Exception {
        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder()
                        .setJobId(jobId)
                        .addJobVertices(jobVertices)
                        .build();

        final DefaultSchedulerBuilder builder =
                new DefaultSchedulerBuilder(jobGraph, mainThreadExecutor, scheduledExecutor)
                        .setRestartBackoffTimeStrategy(new TestRestartBackoffTimeStrategy(true, 0))
                        .setBlobWriter(blobWriter)
                        .setIoExecutor(ioExecutor)
                        .setPartitionTracker(partitionTracker)
                        .setJobMasterConfiguration(jobMasterConfiguration);
        return isAdaptive ? builder.buildAdaptiveBatchJobScheduler() : builder.build();
    }

    private static void deployTasks(
            ExecutionGraph executionGraph,
            JobVertexID jobVertexID,
            TestingLogicalSlotBuilder slotBuilder)
            throws JobException, ExecutionException, InterruptedException {

        for (ExecutionVertex vertex :
                Objects.requireNonNull(executionGraph.getJobVertex(jobVertexID))
                        .getTaskVertices()) {
            LogicalSlot slot = slotBuilder.createTestingLogicalSlot();

            Execution execution = vertex.getCurrentExecutionAttempt();
            execution.registerProducedPartitions(slot.getTaskManagerLocation()).get();
            execution.transitionState(ExecutionState.SCHEDULED);

            vertex.tryAssignResource(slot);
            vertex.deploy();
        }
    }

    public static TaskExecutionState createFinishedTaskExecutionState(
            ExecutionAttemptID attemptId,
            Map<IntermediateResultPartitionID, ResultPartitionBytes> resultPartitionBytes) {
        return new TaskExecutionState(
                attemptId,
                ExecutionState.FINISHED,
                null,
                null,
                new IOMetrics(0, 0, 0, 0, 0, 0, 0, resultPartitionBytes));
    }

    public static TaskExecutionState createFinishedTaskExecutionState(
            ExecutionAttemptID attemptId) {
        return createFinishedTaskExecutionState(attemptId, Collections.emptyMap());
    }

    public static TaskExecutionState createFailedTaskExecutionState(
            ExecutionAttemptID attemptId, Throwable failureCause) {
        return new TaskExecutionState(attemptId, ExecutionState.FAILED, failureCause);
    }

    public static TaskExecutionState createFailedTaskExecutionState(ExecutionAttemptID attemptId) {
        return createFailedTaskExecutionState(attemptId, new Exception("Expected failure cause"));
    }

    public static TaskExecutionState createCanceledTaskExecutionState(
            ExecutionAttemptID attemptId) {
        return new TaskExecutionState(attemptId, ExecutionState.CANCELED);
    }

    private static void runUnsafe(
            final RunnableWithException callback, final ComponentMainThreadExecutor executor) {
        try {
            CompletableFuture.runAsync(
                            () -> {
                                try {
                                    callback.run();
                                } catch (Throwable e) {
                                    throw new RuntimeException(
                                            "Exception shouldn't happen here.", e);
                                }
                            },
                            executor)
                    .join();
        } catch (Exception e) {
            throw new RuntimeException("Exception shouldn't happen here.", e);
        }
    }
}
