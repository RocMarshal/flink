package org.apache.flink.connector.jdbc.sink;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.sink.writer.JdbcGenericWriter;
import org.apache.flink.connector.jdbc.sink.writer.JdbcRowWriter;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterConfig;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterState;
import org.apache.flink.connector.jdbc.sink.writer.RecordExtractor;
import org.apache.flink.connector.jdbc.sink.writer.StatementExecutorFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.connector.jdbc.sink.writer.JdbcRowWriter.createRowExecutor;

public class JdbcRowSink extends JdbcSink<Row> {

    private int[] typesArray;
    private String sql;

    public JdbcRowSink(JdbcWriterConfig jdbcWriterConfig, String sql, int[] typesArray) {
        super(
                jdbcWriterConfig,
                new StatementExecutorFactory<JdbcBatchStatementExecutor<Row>>() {
                    @Override
                    public JdbcBatchStatementExecutor<Row> apply(InitContext initContext) {
                        return createRowExecutor(sql, typesArray, initContext);
                    }
                },
                RecordExtractor.identity());
        this.typesArray = typesArray;
        this.sql = sql;
    }

    @Override
    public JdbcGenericWriter<Row, ?, ?> createWriter(InitContext context) throws IOException {
        return new JdbcRowWriter(
                context,
                typesArray,
                sql,
                JdbcWriterState.empty(),
                renderJdbcConnectionProvider(),
                jdbcWriterConfig,
                RecordExtractor.identity());
    }

    @Override
    public StatefulSinkWriter<Row, JdbcWriterState> restoreWriter(
            InitContext context, Collection<JdbcWriterState> recoveredState) throws IOException {

        JdbcWriterState initState = JdbcWriterState.empty();

        for (JdbcWriterState jdbcWriterState : recoveredState) {
            initState = initState.merge(jdbcWriterState);
        }

        return new JdbcRowWriter(
                context,
                null,
                sql,
                initState,
                renderJdbcConnectionProvider(),
                jdbcWriterConfig,
                RecordExtractor.identity());
    }

    public static JdbcRowSinkBuilder jdbcRowSinkBuilder() {
        return new JdbcRowSinkBuilder();
    }

    /** Builder for {@link JdbcRowSink}. */
    public static class JdbcRowSinkBuilder {
        private String query;
        private int[] typesArray;
        private JdbcWriterConfig jdbcWriterConfig;

        private JdbcStatementBuilder<Row> jdbcStatementBuilder;

        private JdbcRowSinkBuilder() {}

        public JdbcRowSinkBuilder setQuery(String query) {
            this.query = query;
            return this;
        }

        public JdbcRowSinkBuilder setTypesArray(int[] typesArray) {
            this.typesArray = typesArray;
            return this;
        }

        public JdbcRowSinkBuilder setJdbcWriterConfig(JdbcWriterConfig jdbcWriterConfig) {
            this.jdbcWriterConfig = jdbcWriterConfig;
            return this;
        }

        public JdbcRowSinkBuilder setJdbcStatementBuilder(
                JdbcStatementBuilder<Row> jdbcStatementBuilder) {
            this.jdbcStatementBuilder = Preconditions.checkNotNull(jdbcStatementBuilder);
            return this;
        }

        public JdbcRowSink buildSink() {
            return new JdbcRowSink(jdbcWriterConfig, query, typesArray);
        }
    }
}
