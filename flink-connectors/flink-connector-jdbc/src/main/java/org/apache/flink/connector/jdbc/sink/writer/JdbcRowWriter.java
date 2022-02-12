package org.apache.flink.connector.jdbc.sink.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.types.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

import static org.apache.flink.connector.jdbc.utils.JdbcUtils.setRecordToStatement;

public class JdbcRowWriter extends JdbcGenericWriter<Row, Row, JdbcBatchStatementExecutor<Row>> {
    public JdbcRowWriter(
            Sink.InitContext initContext,
            int[] typesArray,
            String sql,
            JdbcWriterState jdbcWriterState,
            JdbcConnectionProvider connectionProvider,
            JdbcWriterConfig options,
            RecordExtractor<Row, Row> jdbcRecordExtractor) {
        super(
                initContext,
                jdbcWriterState,
                connectionProvider,
                options,
                ctx -> createRowExecutor(sql, typesArray, ctx),
                jdbcRecordExtractor);
    }

    @VisibleForTesting
    public static JdbcBatchStatementExecutor<Row> createRowExecutor(
            String sql, int[] typesArray, Sink.InitContext ctx) {
        JdbcStatementBuilder<Row> statementBuilder =
                new JdbcStatementBuilder<Row>() {
                    @Override
                    public void accept(PreparedStatement st, Row record) throws SQLException {
                        setRecordToStatement(st, typesArray, record);
                    }
                };
        return JdbcBatchStatementExecutor.simple(
                sql,
                statementBuilder,
                ctx.getExecutionConfig().isObjectReuseEnabled() ? Row::copy : Function.identity());
    }
}
