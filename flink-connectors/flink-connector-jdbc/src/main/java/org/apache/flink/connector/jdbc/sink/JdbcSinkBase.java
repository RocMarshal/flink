package org.apache.flink.connector.jdbc.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.sink.committer.JdbcCommittable;
import org.apache.flink.connector.jdbc.sink.committer.JdbcCommittableSerializer;
import org.apache.flink.connector.jdbc.sink.committer.JdbcCommitter;
import org.apache.flink.connector.jdbc.sink.writer.JdbcGenericWriter;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterConfig;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterState;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterStateSerializer;
import org.apache.flink.connector.jdbc.sink.writer.RecordExtractor;
import org.apache.flink.connector.jdbc.sink.writer.StatementExecutorFactory;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatementImpl;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.connector.jdbc.xa.XaFacade;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.function.Function;

import static org.apache.flink.connector.jdbc.utils.JdbcUtils.setRecordToStatement;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** JdbcSink. */
public class JdbcSinkBase<IN, JdbcIn, JdbcExec extends JdbcBatchStatementExecutor<JdbcIn>>
        implements StatefulSink<IN, JdbcWriterState>, TwoPhaseCommittingSink<IN, JdbcCommittable> {

    protected JdbcWriterConfig jdbcWriterConfig;

    protected StatementExecutorFactory<JdbcExec> statementExecutorFactory;

    protected RecordExtractor<IN, JdbcIn> recordExtractor;

    public JdbcSinkBase(
            JdbcWriterConfig jdbcWriterConfig,
            StatementExecutorFactory<JdbcExec> statementExecutorFactory,
            RecordExtractor<IN, JdbcIn> recordExtractor) {
        this.jdbcWriterConfig = Preconditions.checkNotNull(jdbcWriterConfig);
        this.statementExecutorFactory = Preconditions.checkNotNull(statementExecutorFactory);
        this.recordExtractor = recordExtractor;
    }

    protected JdbcConnectionProvider renderJdbcConnectionProvider() {
        SerializableSupplier<XADataSource> xaDatasourceSupplier =
                jdbcWriterConfig.getJdbcConnectionOptions().getXaDatasourceSupplier();
        if (xaDatasourceSupplier != null) {
            return XaFacade.fromXaDataSourceSupplier(
                    xaDatasourceSupplier,
                    jdbcWriterConfig.getJdbcExactlyOnceOptions().getTimeoutSec(),
                    jdbcWriterConfig.getJdbcExactlyOnceOptions().isTransactionPerConnection());
        }
        return new SimpleJdbcConnectionProvider(jdbcWriterConfig.getJdbcConnectionOptions());
    }

    @Override
    public JdbcGenericWriter<IN, ?, ?> createWriter(InitContext context) throws IOException {
        // TODO 转换 传入的 connection provider
        return new JdbcGenericWriter<IN, JdbcIn, JdbcExec>(
                context,
                JdbcWriterState.empty(),
                renderJdbcConnectionProvider(),
                jdbcWriterConfig,
                statementExecutorFactory,
                recordExtractor) {};
    }

    @Override
    public Committer<JdbcCommittable> createCommitter() throws IOException {
        return new JdbcCommitter(null, jdbcWriterConfig);
    }

    @Override
    public Committer<JdbcCommittable> createCommitter(RuntimeContext runtimeContext) throws IOException {
        return new JdbcCommitter(runtimeContext, jdbcWriterConfig);
    }

    @Override
    public SimpleVersionedSerializer<JdbcCommittable> getCommittableSerializer() {
        return new JdbcCommittableSerializer();
    }

    protected JdbcWriterState mergeState(Collection<JdbcWriterState> jdbcWriterStates) {
        JdbcWriterState initState = JdbcWriterState.empty();

        for (JdbcWriterState jdbcWriterState : jdbcWriterStates) {
            initState = initState.merge(jdbcWriterState);
        }
        return initState;
    }

    @Override
    public StatefulSinkWriter<IN, JdbcWriterState> restoreWriter(
            InitContext context, Collection<JdbcWriterState> recoveredState) throws IOException {

        return new JdbcGenericWriter<IN, JdbcIn, JdbcExec>(
                context,
                mergeState(recoveredState),
                renderJdbcConnectionProvider(),
                jdbcWriterConfig,
                statementExecutorFactory,
                recordExtractor) {};
    }

    @Override
    public SimpleVersionedSerializer<JdbcWriterState> getWriterStateSerializer() {
        return new JdbcWriterStateSerializer();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private JdbcWriterConfig jdbcWriterConfig;

        private String[] fieldNames;
        private String[] keyFields;
        private int[] fieldTypes;

        public Builder setJdbcWriterConfig(JdbcWriterConfig jdbcWriterConfig) {
            this.jdbcWriterConfig = jdbcWriterConfig;
            return this;
        }

        /** required, field names of this jdbc sink. */
        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        /** required, upsert unique keys. */
        public Builder setKeyFields(String[] keyFields) {
            this.keyFields = keyFields;
            return this;
        }

        /** required, field types of this jdbc sink. */
        public Builder setFieldTypes(int[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured JdbcUpsertOutputFormat
         */
        public JdbcSinkBase<Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>> build() {
            checkNotNull(jdbcWriterConfig, "No options supplied.");
            checkNotNull(fieldNames, "No fieldNames supplied.");
            JdbcDmlOptions dml =
                    JdbcDmlOptions.builder()
                            .withTableName(
                                    jdbcWriterConfig
                                            .getJdbcConnectionOptions()
                                            .convertJdbcConnectorOptions()
                                            .getTableName())
                            .withDialect(
                                    jdbcWriterConfig
                                            .getJdbcConnectionOptions()
                                            .convertJdbcConnectorOptions()
                                            .getDialect())
                            .withFieldNames(fieldNames)
                            .withKeyFields(keyFields)
                            .withFieldTypes(fieldTypes)
                            .build();
            if (dml.getKeyFields().isPresent() && dml.getKeyFields().get().length > 0) {
                return new JdbcTableUpsertSink(jdbcWriterConfig, dml);
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
                return new JdbcSinkBase<Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>>(
                        jdbcWriterConfig,
                        new StatementExecutorFactory<JdbcBatchStatementExecutor<Row>>() {
                            @Override
                            public JdbcBatchStatementExecutor<Row> apply(InitContext initContext) {
                                return createSimpleRowExecutor(
                                        sql,
                                        dml.getFieldTypes(),
                                        initContext.getExecutionConfig().isObjectReuseEnabled());
                            }
                        },
                        (RecordExtractor<Tuple2<Boolean, Row>, Row>)
                                tuple2 -> {
                                    Preconditions.checkArgument(tuple2.f0);
                                    return tuple2.f1;
                                });
            }
        }
    }

    /**
     * Creates a {@link JdbcStatementBuilder} for {@link Row} using the provided SQL types array.
     * Uses {@link JdbcUtils#setRecordToStatement}
     */
    static JdbcStatementBuilder<Row> createRowJdbcStatementBuilder(int[] types) {
        return (st, record) -> setRecordToStatement(st, types, record);
    }

    static JdbcBatchStatementExecutor<Row> createSimpleRowExecutor(
            String sql, int[] fieldTypes, boolean objectReuse) {
        return JdbcBatchStatementExecutor.simple(
                sql,
                createRowJdbcStatementBuilder(fieldTypes),
                objectReuse ? Row::copy : Function.identity());
    }
}
