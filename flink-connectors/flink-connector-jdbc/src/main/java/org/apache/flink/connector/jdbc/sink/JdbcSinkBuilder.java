package org.apache.flink.connector.jdbc.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterConfig;
import org.apache.flink.connector.jdbc.sink.writer.RecordExtractor;
import org.apache.flink.connector.jdbc.sink.writer.StatementExecutorFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.flink.connector.jdbc.sink.writer.JdbcGenericWriter.createBufferReduceExecutor;
import static org.apache.flink.connector.jdbc.sink.writer.JdbcGenericWriter.createSimpleBufferedExecutor;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class JdbcSinkBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    private JdbcConnectorOptions jdbcOptions;
    private JdbcExecutionOptions executionOptions;
    private JdbcDmlOptions dmlOptions;

    private JdbcExactlyOnceOptions jdbcExactlyOnceOptions;
    private TypeInformation<RowData> rowDataTypeInformation;
    private DataType[] fieldDataTypes;

    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.NONE;

    private SerializableSupplier<XADataSource> xaDataSourceSupplier;

    public JdbcSinkBuilder() {}

    public JdbcSinkBuilder setJdbcOptions(JdbcConnectorOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
        return this;
    }

    public JdbcSinkBuilder setExactlyOnceOption(
            SerializableSupplier<XADataSource> xaDataSourceSupplier,
            JdbcExactlyOnceOptions jdbcExactlyOnceOptions) {
        this.deliveryGuarantee = DeliveryGuarantee.EXACTLY_ONCE;
        this.xaDataSourceSupplier = xaDataSourceSupplier;
        this.jdbcExactlyOnceOptions = jdbcExactlyOnceOptions;
        return this;
    }

    public JdbcSinkBuilder setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = deliveryGuarantee;
        return this;
    }

    public JdbcSinkBuilder setJdbcExecutionOptions(JdbcExecutionOptions executionOptions) {
        this.executionOptions = executionOptions;
        return this;
    }

    public JdbcSinkBuilder setJdbcDmlOptions(JdbcDmlOptions dmlOptions) {
        this.dmlOptions = dmlOptions;
        return this;
    }

    public JdbcSinkBuilder setRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
        this.rowDataTypeInformation = rowDataTypeInfo;
        return this;
    }

    public JdbcSinkBuilder setFieldDataTypes(DataType[] fieldDataTypes) {
        this.fieldDataTypes = fieldDataTypes;
        return this;
    }

    public JdbcSinkBase<RowData, ?, ?> build() {
        checkNotNull(jdbcOptions, "jdbc options can not be null");
        checkNotNull(dmlOptions, "jdbc dml options can not be null");
        checkNotNull(executionOptions, "jdbc execution options can not be null");

        final LogicalType[] logicalTypes =
                Arrays.stream(fieldDataTypes)
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new);
        if (dmlOptions.getKeyFields().isPresent() && dmlOptions.getKeyFields().get().length > 0) {
            // upsert query
            return new JdbcSinkBase<>(
                    JdbcWriterConfig.builder()
                            .setJdbcConnectionOptions(jdbcOptions)
                            .setJdbcExecutionOptions(JdbcExecutionOptions.builder().build())
                            .build(),
                    new StatementExecutorFactory<JdbcBatchStatementExecutor<RowData>>() {
                        @Override
                        public JdbcBatchStatementExecutor<RowData> apply(
                                Sink.InitContext initContext) {
                            return createBufferReduceExecutor(
                                    dmlOptions, initContext, rowDataTypeInformation, logicalTypes);
                        }
                    },
                    RecordExtractor.identity());
        } else {
            // append only query
            final String sql =
                    dmlOptions
                            .getDialect()
                            .getInsertIntoStatement(
                                    dmlOptions.getTableName(), dmlOptions.getFieldNames());
            return new JdbcSinkBase<>(
                    JdbcWriterConfig.builder()
                            .setJdbcConnectionOptions(jdbcOptions)
                            .setJdbcExecutionOptions(JdbcExecutionOptions.builder().build())
                            .build(),
                    ctx ->
                            createSimpleBufferedExecutor(
                                    ctx,
                                    dmlOptions.getDialect(),
                                    dmlOptions.getFieldNames(),
                                    logicalTypes,
                                    sql,
                                    rowDataTypeInformation),
                    RecordExtractor.identity());
        }
    }
}
