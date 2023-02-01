package org.apache.flink.connector.jdbc.sinkxa.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.sinkxa.committer.JdbcCommittable;
import org.apache.flink.connector.jdbc.xa.CheckpointAndXid;
import org.apache.flink.connector.jdbc.xa.JdbcXaSinkFunction;
import org.apache.flink.connector.jdbc.xa.XaFacade;
import org.apache.flink.connector.jdbc.xa.XaGroupOps;
import org.apache.flink.connector.jdbc.xa.XaGroupOpsImpl;
import org.apache.flink.connector.jdbc.xa.XaSinkStateHandler;
import org.apache.flink.connector.jdbc.xa.XaSinkStateHandlerImpl;
import org.apache.flink.connector.jdbc.xa.XidGenerator;

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
import java.util.function.Function;

public class JdbcWriter<IN>
        implements StatefulSink.StatefulSinkWriter<IN, JdbcWriterState>, TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, JdbcCommittable> {

    public static final Logger LOG = LoggerFactory.getLogger(JdbcWriter.class);

    private final JdbcOutputFormat<IN, IN, JdbcBatchStatementExecutor<IN>> jdbcOuput;

    private final XaFacade xaFacade;
    private final XaGroupOps xaGroupOps;
    private final XidGenerator xidGenerator;
    private final JdbcExactlyOnceOptions options;
    private final Sink.InitContext initContext;
    private final JdbcWriterState jdbcWriterState;

    private transient List<CheckpointAndXid> preparedXids = new ArrayList<>();
    private transient Deque<Xid> hangingXids = new LinkedList<>();
    private transient Xid currentXid;

    public JdbcXaSinkFunction(
            String sql,
            JdbcStatementBuilder<T> statementBuilder,
            XaFacade xaFacade,
            JdbcExecutionOptions executionOptions,
            JdbcExactlyOnceOptions options) {
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
                new XaSinkStateHandlerImpl(),
                options,
                new XaGroupOpsImpl(xaFacade));
    }

    public JdbcXaSinkFunction(
            JdbcOutputFormat<T, T, JdbcBatchStatementExecutor<T>> outputFormat,
            XaFacade xaFacade,
            XidGenerator xidGenerator,
            XaSinkStateHandler stateHandler,
            JdbcExactlyOnceOptions options,
            XaGroupOps xaGroupOps) {

        Preconditions.checkArgument(
                outputFormat.getExecutionOptions().getMaxRetries() == 0,
                "JDBC XA sink requires maxRetries equal to 0, otherwise it could "
                        + "cause duplicates. See issue FLINK-22311 for details.");

        this.xaFacade = Preconditions.checkNotNull(xaFacade);
        this.xidGenerator = Preconditions.checkNotNull(xidGenerator);
        this.outputFormat = Preconditions.checkNotNull(outputFormat);
        this.stateHandler = Preconditions.checkNotNull(stateHandler);
        this.options = Preconditions.checkNotNull(options);
        this.xaGroupOps = xaGroupOps;
    }

    public JdbcWriter(
            Sink.InitContext initContext,
            JdbcWriterState jdbcWriterState,
            JdbcOutputFormat<IN, IN, JdbcBatchStatementExecutor<IN>> jdbcOuput,
            XaFacade xaFacade,
            XaGroupOps xaGroupOps,
            XidGenerator xidGenerator,
            JdbcExactlyOnceOptions options) {
        this.jdbcWriterState = jdbcWriterState;
        this.initContext = initContext;
        this.jdbcOuput = jdbcOuput;
        this.xaFacade = xaFacade;
        this.xaGroupOps = xaGroupOps;
        this.xidGenerator = xidGenerator;
        this.options = options;
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        jdbcOuput.writeRecord(element);
    }


    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        jdbcOuput.flush();
    }

    @Override
    public List<JdbcWriterState> snapshotState(long checkpointId) throws IOException {
        return Collections.singletonList(JdbcWriterState.of(preparedXids, hangingXids));
    }

    @Override
    public void close() throws Exception {
        jdbcOuput.close();
    }

    @Override
    public Collection<JdbcCommittable> prepareCommit() throws IOException, InterruptedException {
        return null;
    }
}
