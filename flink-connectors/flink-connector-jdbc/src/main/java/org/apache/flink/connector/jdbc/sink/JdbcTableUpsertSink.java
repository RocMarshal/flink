package org.apache.flink.connector.jdbc.sink;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.executor.InsertOrUpdateJdbcExecutor;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.sink.writer.JdbcGenericWriter;
import org.apache.flink.connector.jdbc.sink.writer.JdbcTableUpsertWriter;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterConfig;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterState;
import org.apache.flink.connector.jdbc.sink.writer.RecordExtractor;
import org.apache.flink.connector.jdbc.sink.writer.StatementExecutorFactory;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatementImpl;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.function.Function;

import static org.apache.flink.connector.jdbc.utils.JdbcUtils.getPrimaryKey;
import static org.apache.flink.connector.jdbc.utils.JdbcUtils.setRecordToStatement;
import static org.apache.flink.util.Preconditions.checkArgument;

/** A class. */
public class JdbcTableUpsertSink
        extends JdbcSinkBase<Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>> {

    private JdbcDmlOptions dml;

    private final StatementExecutorFactory<JdbcBatchStatementExecutor<Row>>
            deleteStatementExecutorFactory;

    public JdbcTableUpsertSink(JdbcWriterConfig jdbcWriterConfig, JdbcDmlOptions jdbcDmlOptions) {
        this(
                jdbcWriterConfig,
                ctx -> createUpsertRowExecutor(jdbcDmlOptions, ctx),
                ctx -> createDeleteExecutor(jdbcDmlOptions, ctx));
    }

    @VisibleForTesting
    public JdbcTableUpsertSink(
            JdbcWriterConfig jdbcWriterConfig,
            StatementExecutorFactory<JdbcBatchStatementExecutor<Row>> statementExecutorFactory,
            StatementExecutorFactory<JdbcBatchStatementExecutor<Row>>
                    deleteStatementExecutorFactory) {
        super(
                jdbcWriterConfig,
                statementExecutorFactory,
                new RecordExtractor<Tuple2<Boolean, Row>, Row>() {
                    @Override
                    public Row apply(Tuple2<Boolean, Row> booleanRowTuple2) {
                        return booleanRowTuple2.f1;
                    }
                });
        this.deleteStatementExecutorFactory = deleteStatementExecutorFactory;
    }

    @Override
    public JdbcGenericWriter<Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>>
            createWriter(InitContext context) throws IOException {
        return getWriter(context, JdbcWriterState.empty());
    }

    @Override
    public StatefulSinkWriter<Tuple2<Boolean, Row>, JdbcWriterState> restoreWriter(
            InitContext context, Collection<JdbcWriterState> recoveredState) throws IOException {
        JdbcWriterState jdbcWriterState = mergeState(recoveredState);
        return getWriter(context, jdbcWriterState);
    }

    private JdbcGenericWriter<Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>> getWriter(
            InitContext context, JdbcWriterState jdbcWriterState) {

        if (dml == null) {
            return new JdbcTableUpsertWriter(
                    context,
                    jdbcWriterState,
                    renderJdbcConnectionProvider(),
                    jdbcWriterConfig,
                    statementExecutorFactory,
                    deleteStatementExecutorFactory,
                    new RecordExtractor<Tuple2<Boolean, Row>, Row>() {
                        @Override
                        public Row apply(Tuple2<Boolean, Row> booleanRowTuple2) {
                            return Row.of();
                        }
                    });
        }

        if (dml.getKeyFields().isPresent() && dml.getKeyFields().get().length > 0) {
            return new JdbcTableUpsertWriter(
                    context,
                    jdbcWriterState,
                    renderJdbcConnectionProvider(),
                    jdbcWriterConfig,
                    dml,
                    new RecordExtractor<Tuple2<Boolean, Row>, Row>() {
                        @Override
                        public Row apply(Tuple2<Boolean, Row> booleanRowTuple2) {
                            return Row.of();
                        }
                    });
        } else {
            // warn: don't close over builder fields
            String sql =
                    FieldNamedPreparedStatementImpl.parseNamedStatement(
                            jdbcWriterConfig
                                    .getJdbcConnectionOptions()
                                    .convertJdbcConnectorOptions()
                                    .getDialect()
                                    .getInsertIntoStatement(
                                            dml.getTableName(), dml.getFieldNames()),
                            new HashMap<>());
            return new JdbcGenericWriter<
                    Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>>(
                    context,
                    jdbcWriterState,
                    renderJdbcConnectionProvider(),
                    jdbcWriterConfig,
                    (StatementExecutorFactory<JdbcBatchStatementExecutor<Row>>)
                            initContext ->
                                    createSimpleRowExecutor(
                                            sql,
                                            dml.getFieldTypes(),
                                            initContext
                                                    .getExecutionConfig()
                                                    .isObjectReuseEnabled()),
                    (RecordExtractor<Tuple2<Boolean, Row>, Row>)
                            tuple2 -> {
                                Preconditions.checkArgument(tuple2.f0);
                                return tuple2.f1;
                            }) {};
        }
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
        return row -> getPrimaryKey(row, pkFields);
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

    /**
     * Creates a {@link JdbcStatementBuilder} for {@link Row} using the provided SQL types array.
     * Uses {@link JdbcUtils#setRecordToStatement}
     */
    public static JdbcStatementBuilder<Row> createRowJdbcStatementBuilder(int[] types) {
        return (st, record) -> setRecordToStatement(st, types, record);
    }
}
