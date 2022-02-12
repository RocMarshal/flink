/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc2.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataLookupFunction;
import org.apache.flink.connector.jdbc2.source.JdbcSource;
import org.apache.flink.connector.jdbc2.source.JdbcSourceBuilder;
import org.apache.flink.connector.jdbc2.source.enumerator.SqlTemplateSplitEnumerator;
import org.apache.flink.connector.jdbc2.source.reader.extractor.RowDataResultExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;

/** A {@link DynamicTableSource} for JDBC. */
@Internal
public class JdbcDynamicTableSource
        implements ScanTableSource,
                LookupTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown,
                SupportsWatermarkPushDown {

    private static final String JDBC_TRANSFORMATION = "jdbc2";

    private final JdbcConnectorOptions options;
    private final JdbcReadOptions readOptions;
    private final JdbcLookupOptions lookupOptions;
    private DataType physicalRowDataType;
    private final String dialectName;
    private long limit = -1;
    private @Nullable WatermarkStrategy<RowData> watermarkStrategy;

    private final String tableIdentifier;

    public JdbcDynamicTableSource(
            JdbcConnectorOptions options,
            JdbcReadOptions readOptions,
            JdbcLookupOptions lookupOptions,
            DataType physicalRowDataType,
            String tableIdentifier) {
        this.options = options;
        this.readOptions = readOptions;
        this.lookupOptions = lookupOptions;
        this.physicalRowDataType = physicalRowDataType;
        this.dialectName = options.getDialect().dialectName();
        this.tableIdentifier = Preconditions.checkNotNull(tableIdentifier);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // JDBC only support non-nested look up keys
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = DataType.getFieldNames(physicalRowDataType).get(innerKeyArr[0]);
        }
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();

        return TableFunctionProvider.of(
                new JdbcRowDataLookupFunction(
                        options,
                        lookupOptions,
                        DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                        DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]),
                        keyNames,
                        rowType));
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        JdbcSourceBuilder<RowData> builder = JdbcSource.builder();

        builder.setDriverName(options.getDriverName())
                .setDBUrl(options.getDbURL())
                .setUsername(options.getUsername().orElse(null))
                .setPassword(options.getPassword().orElse(null))
                .setAutoCommit(readOptions.getAutoCommit());
        if (readOptions.getFetchSize() != 0) {
            builder.setResultSetFetchSize(readOptions.getFetchSize());
        }
        final JdbcDialect dialect = options.getDialect();
        String query =
                dialect.getSelectFromStatement(
                        options.getTableName(),
                        DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                        new String[0]);
        org.apache.flink.connector.jdbc2.source.enumerator.parameter.provider
                        .JdbcNumericBetweenParametersProvider
                provider = null;
        if (readOptions.getPartitionColumnName().isPresent()) {
            long lowerBound = readOptions.getPartitionLowerBound().get();
            long upperBound = readOptions.getPartitionUpperBound().get();
            int numPartitions = readOptions.getNumPartitions().get();
            provider =
                    new org.apache.flink.connector.jdbc2.source.enumerator.parameter.provider
                                    .JdbcNumericBetweenParametersProvider(lowerBound, upperBound)
                            .ofBatchNum(numPartitions);
            query +=
                    " WHERE "
                            + dialect.quoteIdentifier(readOptions.getPartitionColumnName().get())
                            + " BETWEEN ? AND ?";
        }
        if (limit >= 0) {
            query = String.format("%s %s", query, dialect.getLimitClause(limit));
        }
        builder.setSqlSplitEnumeratorProvider(
                new SqlTemplateSplitEnumerator.TemplateSqlSplitEnumeratorProvider()
                        .setSqlTemplate(query)
                        .setParameterValuesProvider(provider));
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        builder.setResultExtractor(new RowDataResultExtractor(dialect.getRowConverter(rowType)));
        builder.setTypeInformation(
                runtimeProviderContext.createTypeInformation(physicalRowDataType));
        JdbcSource<RowData> jdbcSource = builder.build();

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                if (watermarkStrategy == null) {
                    watermarkStrategy = WatermarkStrategy.noWatermarks();
                }
                DataStreamSource<RowData> sourceStream =
                        execEnv.fromSource(
                                jdbcSource, watermarkStrategy, "JdbcSource-" + tableIdentifier);
                providerContext.generateUid(JDBC_TRANSFORMATION).ifPresent(sourceStream::uid);
                return sourceStream;
            }

            @Override
            public boolean isBounded() {
                return jdbcSource.getBoundedness() == Boundedness.BOUNDED;
            }
        };
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public boolean supportsNestedProjection() {
        // JDBC doesn't support nested projection
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
    }

    @Override
    public DynamicTableSource copy() {
        JdbcDynamicTableSource jdbcDynamicTableSource =
                new JdbcDynamicTableSource(
                        options, readOptions, lookupOptions, physicalRowDataType, tableIdentifier);
        jdbcDynamicTableSource.watermarkStrategy = this.watermarkStrategy;
        return jdbcDynamicTableSource;
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + dialectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JdbcDynamicTableSource)) {
            return false;
        }
        JdbcDynamicTableSource that = (JdbcDynamicTableSource) o;
        return Objects.equals(options, that.options)
                && Objects.equals(readOptions, that.readOptions)
                && Objects.equals(lookupOptions, that.lookupOptions)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(dialectName, that.dialectName)
                && Objects.equals(limit, that.limit)
                && Objects.equals(tableIdentifier, that.tableIdentifier)
                && Objects.equals(watermarkStrategy, that.watermarkStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                options,
                readOptions,
                lookupOptions,
                physicalRowDataType,
                dialectName,
                limit,
                tableIdentifier,
                watermarkStrategy);
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }
}
