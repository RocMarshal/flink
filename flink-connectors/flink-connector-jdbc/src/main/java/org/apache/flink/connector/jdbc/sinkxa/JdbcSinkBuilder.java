package org.apache.flink.connector.jdbc.sinkxa;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;

public class JdbcSinkBuilder {
    public static <T> JdbcSink<T> exactlyOnceSink(
            String sql,
            JdbcStatementBuilder<T> statementBuilder,
            JdbcExecutionOptions executionOptions,
            JdbcExactlyOnceOptions exactlyOnceOptions,
            SerializableSupplier<XADataSource> dataSourceSupplier,
            TypeInformation<?> typeInformation) {
        return new JdbcSink<>(
                sql,
                statementBuilder,
                dataSourceSupplier,
                executionOptions,
                exactlyOnceOptions,
                typeInformation);
    }
}
