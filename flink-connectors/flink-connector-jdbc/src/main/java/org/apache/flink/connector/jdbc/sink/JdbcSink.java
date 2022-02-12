package org.apache.flink.connector.jdbc.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.sink.writer.JdbcGenericWriter;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterConfig;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterState;
import org.apache.flink.connector.jdbc.sink.writer.RecordExtractor;
import org.apache.flink.connector.jdbc.sink.writer.StatementExecutorFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Function;

public abstract class JdbcSink<T> extends JdbcSinkBase<T, T, JdbcBatchStatementExecutor<T>> {

    protected @Nullable TypeInformation<T> typeInformation;

    public JdbcSink(
            JdbcWriterConfig jdbcWriterConfig,
            StatementExecutorFactory<JdbcBatchStatementExecutor<T>> statementExecutorFactory,
            RecordExtractor<T, T> recordExtractor) {
        super(jdbcWriterConfig, statementExecutorFactory, recordExtractor);
    }

    public JdbcSink(
            String sql,
            JdbcWriterConfig jdbcWriterConfig,
            JdbcStatementBuilder<T> jdbcStatementBuilder,
            @Nullable TypeInformation<T> typeInformation) {
        super(jdbcWriterConfig, new StatementExecutorFactory<JdbcBatchStatementExecutor<T>>() {
            @Override
            public JdbcBatchStatementExecutor<T> apply(InitContext context) {
                return JdbcBatchStatementExecutor.simple(sql, jdbcStatementBuilder,
                        Function.identity());
            }
        }, RecordExtractor.identity());
        if (typeInformation != null) {
            this.typeInformation = typeInformation;
        }
    }

    @Override
    public JdbcGenericWriter<T, ?, ?> createWriter(InitContext context) throws IOException {
        // TODO 转换 传入的 connection provider
        JdbcGenericWriter<T, ?, ?> jdbcGenericWriter = null;

        try {
             jdbcGenericWriter= new JdbcGenericWriter<T, T, JdbcBatchStatementExecutor<T>>(
                    context,
                    JdbcWriterState.empty(),
                    renderJdbcConnectionProvider(),
                    jdbcWriterConfig,
                    statementExecutorFactory,
                    recordExtractor,
                    typeInformation) {};
        } catch (Throwable throwable) {
            System.out.println(throwable.toString());
        }


        return jdbcGenericWriter;
    }

    @Override
    public StatefulSinkWriter<T, JdbcWriterState> restoreWriter(
            InitContext context, Collection<JdbcWriterState> recoveredState) throws IOException {

        return new JdbcGenericWriter<T, T, JdbcBatchStatementExecutor<T>>(
                context,
                mergeState(recoveredState),
                renderJdbcConnectionProvider(),
                jdbcWriterConfig,
                statementExecutorFactory,
                recordExtractor,
                typeInformation) {};
    }
}
