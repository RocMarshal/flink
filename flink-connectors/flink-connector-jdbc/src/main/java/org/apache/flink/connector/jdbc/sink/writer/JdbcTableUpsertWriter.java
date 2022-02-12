package org.apache.flink.connector.jdbc.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.InsertOrUpdateJdbcExecutor;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatementImpl;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.function.Function;

import static org.apache.flink.connector.jdbc.utils.JdbcUtils.setRecordToStatement;
import static org.apache.flink.util.Preconditions.checkArgument;

public class JdbcTableUpsertWriter
        extends JdbcGenericWriter<Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcTableUpsertWriter.class);

    private JdbcBatchStatementExecutor<Row> deleteExecutor;
    private final StatementExecutorFactory<JdbcBatchStatementExecutor<Row>>
            deleteStatementExecutorFactory;

    public JdbcTableUpsertWriter(
            Sink.InitContext initContext,
            JdbcWriterState jdbcWriterState,
            JdbcConnectionProvider connectionProvider,
            JdbcWriterConfig jdbcWriterConfig,
            StatementExecutorFactory<JdbcBatchStatementExecutor<Row>> statementExecutorFactory,
            StatementExecutorFactory<JdbcBatchStatementExecutor<Row>>
                    deleteStatementExecutorFactory,
            RecordExtractor<Tuple2<Boolean, Row>, Row> jdbcRecordExtractor) {
        super(
                initContext,
                jdbcWriterState,
                connectionProvider,
                jdbcWriterConfig,
                statementExecutorFactory,
                jdbcRecordExtractor);
        this.deleteStatementExecutorFactory = deleteStatementExecutorFactory;
        this.deleteExecutor = this.deleteStatementExecutorFactory.apply(initContext);
        try {
            deleteExecutor.prepareStatements(connectionProvider.getConnection());
        } catch (SQLException e) {
            LOGGER.error("Error in prepare", e);
        }
    }

    public JdbcTableUpsertWriter(
            Sink.InitContext initContext,
            JdbcWriterState jdbcWriterState,
            JdbcConnectionProvider connectionProvider,
            JdbcWriterConfig options,
            JdbcDmlOptions dmlOptions,
            RecordExtractor<Tuple2<Boolean, Row>, Row> jdbcRecordExtractor) {
        this(
                initContext,
                jdbcWriterState,
                connectionProvider,
                options,
                ctx -> createUpsertRowExecutor(dmlOptions, ctx),
                ctx -> createDeleteExecutor(dmlOptions, ctx),
                jdbcRecordExtractor);
    }

    @Override
    protected void addToBatch(Tuple2<Boolean, Row> original, Row extracted) throws SQLException {
        if (original.f0) {
            super.addToBatch(original, extracted);
        } else {
            deleteExecutor.addToBatch(extracted);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
        } finally {
            try {
                if (deleteExecutor != null) {
                    deleteExecutor.closeStatements();
                }
            } catch (SQLException e) {
                LOGGER.warn("unable to close delete statement runner", e);
            }
        }
    }

    @Override
    protected void attemptFlush() throws SQLException {
        super.attemptFlush();
        deleteExecutor.executeBatch();
    }

    @Override
    public void updateExecutor(boolean reconnect) throws SQLException, ClassNotFoundException {
        super.updateExecutor(reconnect);
        deleteExecutor.closeStatements();
        deleteExecutor.prepareStatements(connectionProvider.getConnection());
    }

    private static JdbcBatchStatementExecutor<Row> createDeleteExecutor(
            JdbcDmlOptions dmlOptions, Sink.InitContext ctx) {
        int[] pkFields =
                Arrays.stream(dmlOptions.getFieldNames())
                        .mapToInt(Arrays.asList(dmlOptions.getFieldNames())::indexOf)
                        .toArray();
        int[] pkTypes =
                dmlOptions.getFieldTypes() == null
                        ? null
                        : Arrays.stream(pkFields).map(f -> dmlOptions.getFieldTypes()[f]).toArray();
        String deleteSql =
                FieldNamedPreparedStatementImpl.parseNamedStatement(
                        dmlOptions
                                .getDialect()
                                .getDeleteStatement(
                                        dmlOptions.getTableName(), dmlOptions.getFieldNames()),
                        new HashMap<>());
        return createKeyedRowExecutor(pkFields, pkTypes, deleteSql);
    }

    private static String parseNamedStatement(String statement) {
        return FieldNamedPreparedStatementImpl.parseNamedStatement(statement, new HashMap<>());
    }

    private static Function<Row, Row> createRowKeyExtractor(int[] pkFields) {
        return row -> JdbcUtils.getPrimaryKey(row, pkFields);
    }

    private static JdbcBatchStatementExecutor<Row> createKeyedRowExecutor(
            int[] pkFields, int[] pkTypes, String sql) {
        return JdbcBatchStatementExecutor.keyed(
                sql,
                createRowKeyExtractor(pkFields),
                (st, record) ->
                        setRecordToStatement(
                                st, pkTypes, createRowKeyExtractor(pkFields).apply(record)));
    }

    private static JdbcBatchStatementExecutor<Row> createUpsertRowExecutor(
            JdbcDmlOptions opt, Sink.InitContext ctx) {
        checkArgument(opt.getKeyFields().isPresent());

        int[] pkFields =
                Arrays.stream(opt.getKeyFields().get())
                        .mapToInt(Arrays.asList(opt.getFieldNames())::indexOf)
                        .toArray();
        int[] pkTypes =
                opt.getFieldTypes() == null
                        ? null
                        : Arrays.stream(pkFields).map(f -> opt.getFieldTypes()[f]).toArray();

        return opt.getDialect()
                .getUpsertStatement(
                        opt.getTableName(), opt.getFieldNames(), opt.getKeyFields().get())
                .map(
                        sql ->
                                createSimpleRowExecutor(
                                        parseNamedStatement(sql),
                                        opt.getFieldTypes(),
                                        ctx.getExecutionConfig().isObjectReuseEnabled()))
                .orElseGet(
                        () ->
                                new InsertOrUpdateJdbcExecutor<>(
                                        parseNamedStatement(
                                                opt.getDialect()
                                                        .getRowExistsStatement(
                                                                opt.getTableName(),
                                                                opt.getKeyFields().get())),
                                        parseNamedStatement(
                                                opt.getDialect()
                                                        .getInsertIntoStatement(
                                                                opt.getTableName(),
                                                                opt.getFieldNames())),
                                        parseNamedStatement(
                                                opt.getDialect()
                                                        .getUpdateStatement(
                                                                opt.getTableName(),
                                                                opt.getFieldNames(),
                                                                opt.getKeyFields().get())),
                                        createRowJdbcStatementBuilder(pkTypes),
                                        createRowJdbcStatementBuilder(opt.getFieldTypes()),
                                        createRowJdbcStatementBuilder(opt.getFieldTypes()),
                                        createRowKeyExtractor(pkFields),
                                        ctx.getExecutionConfig().isObjectReuseEnabled()
                                                ? Row::copy
                                                : Function.identity()));
    }
}
