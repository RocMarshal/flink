package org.apache.flink.connector.jdbc.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.util.function.SerializableFunction;

/**
 * A factory for creating {@link JdbcBatchStatementExecutor} instance.
 *
 * @param <T> The type of instance.
 */
public interface StatementExecutorFactory<T extends JdbcBatchStatementExecutor<?>>
        extends SerializableFunction<Sink.InitContext, T> {}
