package org.apache.flink.connector.jdbc.sinkxa.writer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.sinkxa.committer.JdbcCommittable;
import org.apache.flink.connector.jdbc.xa.CheckpointAndXid;
import org.apache.flink.connector.jdbc.xa.XaFacade;
import org.apache.flink.connector.jdbc.xa.XaGroupOps;
import org.apache.flink.connector.jdbc.xa.XaGroupOpsImpl;
import org.apache.flink.connector.jdbc.xa.XidGenerator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class JdbcWriter<IN>
        implements StatefulSink.StatefulSinkWriter<IN, JdbcWriterState>,
                TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, JdbcCommittable> {

    public static final Logger LOG = LoggerFactory.getLogger(JdbcWriter.class);

    private final JdbcOutputFormat<IN, IN, JdbcBatchStatementExecutor<IN>> outputFormat;

    private final XaFacade xaFacade;
    private final XaGroupOps xaGroupOps;
    private final XidGenerator xidGenerator;
    private final JdbcExactlyOnceOptions options;
    private final Sink.InitContext initContext;
    private final JdbcWriterState jdbcWriterState;

    private transient List<CheckpointAndXid> preparedXids = new ArrayList<>();
    private transient Deque<Xid> hangingXids = new LinkedList<>();
    private transient Xid currentXid;
    private TypeInformation<?> typeInformation;

    public JdbcWriter(
            String sql,
            JdbcStatementBuilder<IN> statementBuilder,
            XaFacade xaFacade,
            JdbcExecutionOptions executionOptions,
            JdbcExactlyOnceOptions options,
            Sink.InitContext initContext,
            JdbcWriterState jdbcWriterState,
            TypeInformation<?> type) {
        this(
                new JdbcOutputFormat<>(
                        xaFacade,
                        executionOptions,
                        context ->
                                JdbcBatchStatementExecutor.simple(
                                        sql, statementBuilder, Function.identity()),
                        JdbcOutputFormat.RecordExtractor.identity()),
                xaFacade,
                XidGenerator.semanticXidGenerator(),
                options,
                new XaGroupOpsImpl(xaFacade),
                initContext,
                jdbcWriterState,
                type);
    }

    /*public */ JdbcWriter(
            JdbcOutputFormat<IN, IN, JdbcBatchStatementExecutor<IN>> outputFormat,
            XaFacade xaFacade,
            XidGenerator xidGenerator,
            JdbcExactlyOnceOptions options,
            XaGroupOps xaGroupOps,
            Sink.InitContext initContext,
            JdbcWriterState jdbcWriterState,
            TypeInformation<?> type) {
        LOG.error("_____jdbcWriter");

        Preconditions.checkArgument(
                outputFormat.getExecutionOptions().getMaxRetries() == 0,
                "JDBC XA sink requires maxRetries equal to 0, otherwise it could "
                        + "cause duplicates. See issue FLINK-22311 for details.");

        this.xaFacade = Preconditions.checkNotNull(xaFacade);
        this.xidGenerator = Preconditions.checkNotNull(xidGenerator);
        this.outputFormat = Preconditions.checkNotNull(outputFormat);
        this.options = Preconditions.checkNotNull(options);
        this.xaGroupOps = xaGroupOps;
        this.initContext = initContext;
        this.jdbcWriterState = jdbcWriterState;
        this.typeInformation = type;

        xidGenerator.open();
        try {
            if (!xaFacade.isOpen()) {
                xaFacade.open();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        hangingXids = new LinkedList<>(xaGroupOps.failOrRollback(hangingXids).getForRetry());
        // commitUpToCheckpoint(Optional.empty());
        if (options.isDiscoverAndRollbackOnRecovery()) {
            // Pending transactions which are not included into the checkpoint might hold locks and
            // should be rolled back. However, rolling back ALL transactions can cause data loss. So
            // each subtask first commits transactions from its state and then rolls back discovered
            // transactions if they belong to it.
            xaGroupOps.recoverAndRollback(initContext.getRuntimeContext(), xidGenerator);
        }
        try {
            beginTx(0L);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        outputFormat.setRuntimeContext(initContext.getRuntimeContext());
        // open format only after starting the transaction so it gets a ready to  use connection
        try {
            outputFormat.open(
                    initContext.getRuntimeContext().getIndexOfThisSubtask(),
                    initContext.getRuntimeContext().getNumberOfParallelSubtasks());
            outputFormat.setInputType(typeInformation, initContext.getExecutionConfig());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        Preconditions.checkState(currentXid != null, "current xid must not be null");
        if (LOG.isTraceEnabled()) {
            LOG.trace("invoke, xid: {}, value: {}", currentXid, element);
        }
        outputFormat.writeRecord(element);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        outputFormat.flush();
    }

    @Override
    public List<JdbcWriterState> snapshotState(long checkpointId) throws IOException {
        prepareCurrentTx(checkpointId);
        try {
            beginTx(checkpointId + 1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return Collections.singletonList(JdbcWriterState.of(preparedXids, hangingXids));
    }

    @Override
    public void close() throws Exception {
        if (currentXid != null && xaFacade.isOpen()) {
            try {
                LOG.debug("remove current transaction before closing, xid={}", currentXid);
                xaFacade.failAndRollback(currentXid);
            } catch (Exception e) {
                LOG.warn("unable to fail/rollback current transaction, xid={}", currentXid, e);
            }
        }
        xaFacade.close();
        xidGenerator.close();
        // don't format.close(); as we don't want neither to flush nor to close connection here
        currentXid = null;
        hangingXids = null;
        preparedXids = null;
        outputFormat.close();
    }

    @Override
    public Collection<JdbcCommittable> prepareCommit() throws IOException, InterruptedException {
        return null;
    }

    private void prepareCurrentTx(long checkpointId) throws IOException {
        Preconditions.checkState(currentXid != null, "no current xid");
        Preconditions.checkState(
                !hangingXids.isEmpty() && hangingXids.peekLast().equals(currentXid),
                "inconsistent internal state");
        hangingXids.pollLast();
        outputFormat.flush();
        try {
            xaFacade.endAndPrepare(currentXid);
            preparedXids.add(CheckpointAndXid.createNew(checkpointId, currentXid));
        } catch (XaFacade.EmptyXaTransactionException e) {
            LOG.info(
                    "empty XA transaction (skip), xid: {}, checkpoint {}",
                    currentXid,
                    checkpointId);
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
        currentXid = null;
    }
    /** @param checkpointId to associate with the new transaction. */
    private void beginTx(long checkpointId) throws Exception {
        Preconditions.checkState(currentXid == null, "currentXid not null");
        currentXid = xidGenerator.generateXid(initContext.getRuntimeContext(), checkpointId);
        hangingXids.offerLast(currentXid);
        xaFacade.start(currentXid);
        if (checkpointId > 0) {
            // associate outputFormat with a new connection that might have been opened in start()
            outputFormat.updateExecutor(false);
        }
    }

    private void commitUpToCheckpoint(Optional<Long> checkpointInclusive) {
        Tuple2<List<CheckpointAndXid>, List<CheckpointAndXid>> splittedXids =
                split(preparedXids, checkpointInclusive, true);
        if (splittedXids.f0.isEmpty()) {
            checkpointInclusive.ifPresent(
                    cp -> LOG.warn("nothing to commit up to checkpoint: {}", cp));
        } else {
            preparedXids = splittedXids.f1;
            preparedXids.addAll(
                    xaGroupOps
                            .commit(
                                    splittedXids.f0,
                                    options.isAllowOutOfOrderCommits(),
                                    options.getMaxCommitAttempts())
                            .getForRetry());
        }
    }

    private Tuple2<List<CheckpointAndXid>, List<CheckpointAndXid>> split(
            List<CheckpointAndXid> list,
            Optional<Long> checkpointInclusive,
            boolean checkpointIntoLo) {
        return checkpointInclusive
                .map(cp -> split(preparedXids, cp, checkpointIntoLo))
                .orElse(new Tuple2<>(list, new ArrayList<>()));
    }

    private Tuple2<List<CheckpointAndXid>, List<CheckpointAndXid>> split(
            List<CheckpointAndXid> list, long checkpoint, boolean checkpointIntoLo) {
        List<CheckpointAndXid> lo = new ArrayList<>(list.size() / 2);
        List<CheckpointAndXid> hi = new ArrayList<>(list.size() / 2);
        list.forEach(
                i -> {
                    if (i.checkpointId < checkpoint
                            || (i.checkpointId == checkpoint && checkpointIntoLo)) {
                        lo.add(i);
                    } else {
                        hi.add(i);
                    }
                });
        return new Tuple2<>(lo, hi);
    }
}
