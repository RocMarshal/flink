package org.apache.flink.connector.jdbc.sink.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferReducedStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferedStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableInsertOrUpdateStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableSimpleStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.sink.committer.JdbcCommittable;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.connector.jdbc.xa.CheckpointAndXid;
import org.apache.flink.connector.jdbc.xa.SemanticXidGenerator;
import org.apache.flink.connector.jdbc.xa.XaFacade;
import org.apache.flink.connector.jdbc.xa.XaGroupOps;
import org.apache.flink.connector.jdbc.xa.XaGroupOpsImpl;
import org.apache.flink.connector.jdbc.xa.XidGenerator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.transaction.xa.Xid;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.utils.JdbcUtils.setRecordToStatement;
import static org.apache.flink.table.data.RowData.createFieldGetter;
import static org.apache.flink.util.Preconditions.checkArgument;

/** JdbcWriterDelegate. */
public abstract class JdbcGenericWriter<
                IN, JdbcIn, JdbcExec extends JdbcBatchStatementExecutor<JdbcIn>>
        implements StatefulSink.StatefulSinkWriter<IN, JdbcWriterState>,
                TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, JdbcCommittable>,
                Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcGenericWriter.class);

    private final JdbcWriterConfig jdbcWriterConfig;

    private static final long serialVersionUID = 1L;

    private transient JdbcExec jdbcStatementExecutor;

    protected final StatementExecutorFactory<JdbcExec> statementExecutorFactory;
    private transient int batchCount = 0;
    private transient volatile boolean closed = false;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;

    protected Sink.InitContext initContext;

    protected final JdbcConnectionProvider connectionProvider;
    @Nullable private TypeSerializer<IN> serializer;

    private final RecordExtractor<IN, JdbcIn> jdbcRecordExtractor;

    // checkpoints and the corresponding transactions waiting for completion notification from JM
    private long lastCheckpointId;
    //private transient Deque<CheckpointAndXid> preparedXids = new LinkedList<>();
    // hanging XIDs - used for cleanup
    // it's a list to support retries and scaling down
    // possible transaction states: active, idle, prepared
    // last element is the current xid
    private transient Deque<Xid> hangingXids = new LinkedList<>();
    private transient Xid currentXid;
    private @Nullable XaFacade xaFacadedConnection;
    private transient XaGroupOps xaGroupOps;
    private @Nullable XidGenerator xidGenerator;

    protected TypeSerializer<IN>  extractClassInfo(TypeInformation<IN> typeInformation, ExecutionConfig executionConfig) {
        if (typeInformation != null) {
            return typeInformation.createSerializer(executionConfig);
        }
        try {
            Class<?> c = getClass();
            Type genType = c.getGenericSuperclass();
            Type[] genericInterfaces = c.getGenericInterfaces();
            for (Type t : genericInterfaces) {
                System.out.println(t.getTypeName());
            }
            System.out.println(genType.getTypeName());
            Type[] params = ((ParameterizedType) genType).getActualTypeArguments();
            Type t = params[0];
            TypeInformation<IN> typeInfo = (TypeInformation<IN>) TypeExtractor.createTypeInfo(t);
            return typeInfo.createSerializer(executionConfig);
        } catch (Throwable t) {
            LOG.error("Error in extractClassInfo.", t);
            return null;
        }
    }

    public JdbcGenericWriter(
            Sink.InitContext initContext,
            JdbcWriterState jdbcWriterState,
            JdbcConnectionProvider connectionProvider,
            JdbcWriterConfig options,
            StatementExecutorFactory<JdbcExec> statementExecutorFactory,
            RecordExtractor<IN, JdbcIn> jdbcRecordExtractor,
            @Nullable TypeInformation<IN> typeInformation) {

        this.statementExecutorFactory = Preconditions.checkNotNull(statementExecutorFactory);

        this.connectionProvider = connectionProvider;
        this.initContext = initContext;
        this.jdbcWriterConfig = options;
        this.jdbcRecordExtractor = jdbcRecordExtractor;
        if (initContext.getExecutionConfig().isObjectReuseEnabled()) {
            this.serializer = extractClassInfo(typeInformation, initContext.getExecutionConfig());
        }

        // Restore state.
        if (this.jdbcWriterConfig.getDeliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE) {
            this.xaFacadedConnection = connectionProvider.convertXaConnection();
            try {
                this.xaFacadedConnection.open();
                this.xaGroupOps = new XaGroupOpsImpl(xaFacadedConnection);
                this.xidGenerator = new SemanticXidGenerator();
                this.xidGenerator.open();
                if (jdbcWriterState != null) {
                    this.hangingXids = new LinkedList<>(jdbcWriterState.getHanging());
                    this.hangingXids =
                            new LinkedList<>(xaGroupOps.failOrRollback(hangingXids).getForRetry());
                }

                this.lastCheckpointId =
                        initContext
                                .getRestoredCheckpointId()
                                .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
                if (jdbcWriterConfig
                        .getJdbcExactlyOnceOptions()
                        .isDiscoverAndRollbackOnRecovery()) {
                    // Pending transactions which are not included into the checkpoint might hold
                    // locks
                    // and
                    // should be rolled back. However, rolling back ALL transactions can cause data
                    // loss. So
                    // each subtask first commits transactions from its state and then rolls back
                    // discovered
                    // transactions if they belong to it.
                    // xaGroupOps.recoverAndRollback(initContext, xidGenerator);
                }
//                if (hangingXids.isEmpty()) {
                    beginTx(lastCheckpointId + 1);
//                } else {
//                    currentXid = hangingXids.peekLast();
//                }

            } catch (Exception e) {
                System.out.println(e.toString() + "______++++");
//                System.exit(1);

                throw new RuntimeException(e);
            }
        } else {
            this.jdbcStatementExecutor = createAndOpenStatementExecutor(this.statementExecutorFactory);
        }

        if (this.jdbcWriterConfig.getJdbcFlushOptions().getBatchIntervalMs() != 0
                && this.jdbcWriterConfig.getJdbcFlushOptions().getBatchSize() != 1) {
            this.scheduler =
                    Executors.newScheduledThreadPool(
                            1, new ExecutorThreadFactory("jdbc-upsert-output-format"));
            this.scheduledFuture =
                    this.scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (JdbcGenericWriter.this) {
                                    if (!closed) {
                                        try {
                                            flush(false);
                                        } catch (Exception e) {
                                            flushException = e;
                                        }
                                    }
                                }
                            },
                            this.jdbcWriterConfig.getJdbcFlushOptions().getBatchIntervalMs(),
                            this.jdbcWriterConfig.getJdbcFlushOptions().getBatchIntervalMs(),
                            TimeUnit.MILLISECONDS);
        }
    }

    public JdbcGenericWriter(
            Sink.InitContext initContext,
            JdbcWriterState jdbcWriterState,
            JdbcConnectionProvider connectionProvider,
            JdbcWriterConfig options,
            StatementExecutorFactory<JdbcExec> statementExecutorFactory,
            RecordExtractor<IN, JdbcIn> jdbcRecordExtractor) {
        this(
                initContext,
                jdbcWriterState,
                connectionProvider,
                options,
                statementExecutorFactory,
                jdbcRecordExtractor,
                null);
    }

    private void beginTx(long checkpointId) throws Exception {
        Preconditions.checkState(currentXid == null, "currentXid not null");
        currentXid = xidGenerator.generateXid(initContext, checkpointId);
        hangingXids.offerLast(currentXid);
        xaFacadedConnection.start(currentXid);
        this.jdbcStatementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);
        if (checkpointId > 0) {
            // associate outputFormat with a new connection that might have been opened in start()
            updateExecutor(false);
        }
    }

    private JdbcExec createAndOpenStatementExecutor(
            StatementExecutorFactory<JdbcExec> statementExecutorFactory) {
        JdbcExec exec = statementExecutorFactory.apply(initContext);
        try {
            exec.prepareStatements(connectionProvider.getOrEstablishConnection());
        } catch (Throwable t) {
            throw new FlinkRuntimeException("unable to open JDBC writer", t);
        }
        return exec;
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to JDBC failed.", flushException);
        }
    }

    @Override
    public void write(IN record, Context context) throws IOException, InterruptedException {
        checkFlushException();

        try {
            IN recordCopy = copyIfNecessary(record);
            addToBatch(record, jdbcRecordExtractor.apply(recordCopy));
            batchCount++;
            if (jdbcWriterConfig.getJdbcFlushOptions().getBatchSize() > 0
                    && batchCount >= jdbcWriterConfig.getJdbcFlushOptions().getBatchSize()) {
                flush(false);
            }
        } catch (Exception e) {
            throw new IOException("Writing records to JDBC failed.", e);
        }
    }

    private IN copyIfNecessary(IN record) {
        return serializer == null ? record : serializer.copy(record);
    }

    protected void addToBatch(IN original, JdbcIn extracted) throws SQLException {
        jdbcStatementExecutor.addToBatch(extracted);
    }

    private List<CheckpointAndXid> prepareCurrentTx(long checkpointId) throws IOException, InterruptedException {
        LOG.error("______prepareCurrentTx {}", checkpointId);
        Preconditions.checkState(currentXid != null, "no current xid");
        Preconditions.checkState(
                !hangingXids.isEmpty() && hangingXids.peekLast().equals(currentXid),
                "inconsistent internal state");
        Xid xid = hangingXids.pollLast();
        flush(false);
        CheckpointAndXid aNew = null;
        try {
            xaFacadedConnection.endAndPrepare(currentXid);
            aNew = CheckpointAndXid.createNew(checkpointId, currentXid);
            // preparedXids.add(aNew);
        } catch (XaFacade.EmptyXaTransactionException e) {
            LOG.info(
                    "empty XA transaction (skip), xid: {}, checkpoint {}",
                    currentXid,
                    checkpointId);
            hangingXids.add(currentXid);
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
        currentXid = null;
        return Collections.singletonList(aNew);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkFlushException();

        for (int i = 0; i <= jdbcWriterConfig.getJdbcFlushOptions().getMaxRetries(); i++) {
            try {
                attemptFlush();
                batchCount = 0;
                break;
            } catch (SQLException e) {
                LOG.error("JDBC executeBatch error, retry times = {}", i, e);
                if (i >= jdbcWriterConfig.getJdbcFlushOptions().getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    if (!connectionProvider.isConnectionValid()) {
                        updateExecutor(true);
                    }
                } catch (Exception exception) {
                    LOG.error(
                            "JDBC connection is not valid, and reestablish connection failed.",
                            exception);
                    throw new IOException("Reestablish JDBC connection failed", exception);
                }
                try {
                    Thread.sleep(1000L * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }

    protected void attemptFlush() throws SQLException {
        jdbcStatementExecutor.executeBatch();
    }

    @Override
    public List<JdbcWriterState> snapshotState(long checkpointId) throws IOException {
        lastCheckpointId = checkpointId;
        if (jdbcWriterConfig.getDeliveryGuarantee() != DeliveryGuarantee.EXACTLY_ONCE) {
            return Lists.newArrayList();
        }
        LOG.debug("snapshot state, checkpointId={}", checkpointId);

        return Lists.newArrayList(JdbcWriterState.of(new ArrayList<>(), hangingXids));
    }

    @Override
    public Collection<JdbcCommittable> prepareCommit() throws IOException, InterruptedException {
        if (jdbcWriterConfig.getDeliveryGuarantee() != DeliveryGuarantee.EXACTLY_ONCE) {
            return Collections.emptyList();
        }
        List<JdbcCommittable> committables;
        try {
            final long currentChkId = lastCheckpointId + 1;
            committables = prepareCurrentTx(currentChkId).stream().filter(Objects::nonNull)
                    .map(cx-> new JdbcCommittable(xaFacadedConnection, cx)).collect(Collectors.toList());
            beginTx(currentChkId + 1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.error("prepared commit __________, {}", committables);
        LOG.debug("Committing {} committables.", committables);
        return committables;
    }

    @Override
    public void close() throws Exception {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            if (batchCount > 0) {
                try {
                    flush(true);
                } catch (Exception e) {
                    LOG.warn("Writing records to JDBC failed.", e);
                    throw new RuntimeException("Writing records to JDBC failed.", e);
                }
            }

            try {
                if (jdbcStatementExecutor != null) {
                    jdbcStatementExecutor.closeStatements();
                }
            } catch (SQLException e) {
                LOG.warn("Close JDBC writer failed.", e);
            }
        }
        connectionProvider.closeConnection();
        if (jdbcWriterConfig.getDeliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE) {
            xaFacadedConnection.close();
        }
        checkFlushException();
    }

    public static JdbcBatchStatementExecutor<Row> createSimpleRowExecutor(
            String sql, int[] fieldTypes, boolean objectReuse) {
        return JdbcBatchStatementExecutor.simple(
                sql,
                createRowJdbcStatementBuilder(fieldTypes),
                objectReuse ? Row::copy : Function.identity());
    }

    /**
     * Creates a {@link JdbcStatementBuilder} for {@link Row} using the provided SQL types array.
     * Uses {@link JdbcUtils#setRecordToStatement}
     */
    public static JdbcStatementBuilder<Row> createRowJdbcStatementBuilder(int[] types) {
        return (st, record) -> setRecordToStatement(st, types, record);
    }

    public void updateExecutor(boolean reconnect) throws SQLException, ClassNotFoundException {
        jdbcStatementExecutor.closeStatements();
        jdbcStatementExecutor.prepareStatements(
                reconnect
                        ? connectionProvider.reestablishConnection()
                        : connectionProvider.getConnection());
    }

    /** Returns configured {@code JdbcExecutionOptions}. */
    public JdbcExecutionOptions getExecutionOptions() {
        return jdbcWriterConfig.getJdbcFlushOptions();
    }

    @VisibleForTesting
    public Connection getConnection() {
        return connectionProvider.getConnection();
    }

    public static JdbcBatchStatementExecutor<RowData> createBufferReduceExecutor(
            JdbcDmlOptions opt,
            Sink.InitContext ctx,
            TypeInformation<RowData> rowDataTypeInfo,
            LogicalType[] fieldTypes) {
        checkArgument(opt.getKeyFields().isPresent());
        JdbcDialect dialect = opt.getDialect();
        String tableName = opt.getTableName();
        String[] pkNames = opt.getKeyFields().get();
        int[] pkFields =
                Arrays.stream(pkNames)
                        .mapToInt(Arrays.asList(opt.getFieldNames())::indexOf)
                        .toArray();
        LogicalType[] pkTypes =
                Arrays.stream(pkFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);
        final TypeSerializer<RowData> typeSerializer =
                rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
        final Function<RowData, RowData> valueTransform =
                ctx.getExecutionConfig().isObjectReuseEnabled()
                        ? typeSerializer::copy
                        : Function.identity();

        return new TableBufferReducedStatementExecutor(
                createUpsertRowExecutor(
                        dialect,
                        tableName,
                        opt.getFieldNames(),
                        fieldTypes,
                        pkFields,
                        pkNames,
                        pkTypes),
                createDeleteExecutor(dialect, tableName, pkNames, pkTypes),
                createRowKeyExtractor(fieldTypes, pkFields),
                valueTransform);
    }

    public static JdbcBatchStatementExecutor<RowData> createSimpleBufferedExecutor(
            Sink.InitContext ctx,
            JdbcDialect dialect,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            String sql,
            TypeInformation<RowData> rowDataTypeInfo) {
        final TypeSerializer<RowData> typeSerializer =
                rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
        return new TableBufferedStatementExecutor(
                createSimpleRowDataExecutor(dialect, fieldNames, fieldTypes, sql),
                ctx.getExecutionConfig().isObjectReuseEnabled()
                        ? typeSerializer::copy
                        : Function.identity());
    }

    public static JdbcBatchStatementExecutor<RowData> createUpsertRowExecutor(
            JdbcDialect dialect,
            String tableName,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            int[] pkFields,
            String[] pkNames,
            LogicalType[] pkTypes) {
        return dialect.getUpsertStatement(tableName, fieldNames, pkNames)
                .map(sql -> createSimpleRowDataExecutor(dialect, fieldNames, fieldTypes, sql))
                .orElseGet(
                        () ->
                                createInsertOrUpdateExecutor(
                                        dialect,
                                        tableName,
                                        fieldNames,
                                        fieldTypes,
                                        pkFields,
                                        pkNames,
                                        pkTypes));
    }

    public static JdbcBatchStatementExecutor<RowData> createDeleteExecutor(
            JdbcDialect dialect, String tableName, String[] pkNames, LogicalType[] pkTypes) {
        String deleteSql = dialect.getDeleteStatement(tableName, pkNames);
        return createSimpleRowDataExecutor(dialect, pkNames, pkTypes, deleteSql);
    }

    public static JdbcBatchStatementExecutor<RowData> createSimpleRowDataExecutor(
            JdbcDialect dialect, String[] fieldNames, LogicalType[] fieldTypes, final String sql) {
        final JdbcRowConverter rowConverter = dialect.getRowConverter(RowType.of(fieldTypes));
        return new TableSimpleStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(connection, sql, fieldNames),
                rowConverter);
    }

    public static JdbcBatchStatementExecutor<RowData> createInsertOrUpdateExecutor(
            JdbcDialect dialect,
            String tableName,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            int[] pkFields,
            String[] pkNames,
            LogicalType[] pkTypes) {
        final String existStmt = dialect.getRowExistsStatement(tableName, pkNames);
        final String insertStmt = dialect.getInsertIntoStatement(tableName, fieldNames);
        final String updateStmt = dialect.getUpdateStatement(tableName, fieldNames, pkNames);
        return new TableInsertOrUpdateStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection, existStmt, pkNames),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection, insertStmt, fieldNames),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection, updateStmt, fieldNames),
                dialect.getRowConverter(RowType.of(pkTypes)),
                dialect.getRowConverter(RowType.of(fieldTypes)),
                dialect.getRowConverter(RowType.of(fieldTypes)),
                createRowKeyExtractor(fieldTypes, pkFields));
    }

    public static Function<RowData, RowData> createRowKeyExtractor(
            LogicalType[] logicalTypes, int[] pkFields) {
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[pkFields.length];
        for (int i = 0; i < pkFields.length; i++) {
            fieldGetters[i] = createFieldGetter(logicalTypes[pkFields[i]], pkFields[i]);
        }
        return row -> getPrimaryKey(row, fieldGetters);
    }

    public static RowData getPrimaryKey(RowData row, RowData.FieldGetter[] fieldGetters) {
        GenericRowData pkRow = new GenericRowData(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            pkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
        }
        return pkRow;
    }
}
