/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.connector.rabbitmq2.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.rabbitmq2.common.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.source.RabbitMQSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;

/** A RabbitMQ {@link ScanTableSource}. */
@Internal
public class RabbitMQDynamicSource implements ScanTableSource, SupportsWatermarkPushDown {

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    private DataType producedDataType;

    /** Watermark strategy that is used to generate per-partition watermark. */
    private @Nullable WatermarkStrategy<RowData> watermarkStrategy;

    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    /** Data type to configure the formats. */
    private final DataType physicalDataType;

    /** Format for decoding message from RabbitMQ. */
    protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    /** Indices that determine the value fields and the target position in the produced row. */
    protected final int[] valueProjection;

    // --------------------------------------------------------------------------------------------
    // RabbitMQ-specific attributes
    // --------------------------------------------------------------------------------------------

    /** The RabbitMQ queue name to consume. */
    private final String queueName;

    /** The configuration for the RabbitMQ connection. */
    private final RabbitMQConnectionConfig connectionConfig;

    /** The consistency mode for the source. */
    private final ConsistencyMode consistencyMode;

    protected final String tableIdentifier;

    public RabbitMQDynamicSource(
            DataType physicalDataType,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            int[] valueProjection,
            String queueName,
            RabbitMQConnectionConfig connectionConfig,
            ConsistencyMode consistencyMode,
            String tableIdentifier) {
        // Format attributes
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.decodingFormat =
                Preconditions.checkNotNull(
                        decodingFormat, "Value decoding format must not be null.");
        this.valueProjection =
                Preconditions.checkNotNull(valueProjection, "Value projection must not be null.");

        // Mutable attributes
        this.producedDataType = physicalDataType;
        this.watermarkStrategy = null;

        // RabbitMQ0-specific attributes
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(queueName),
                "RabbitMQ queue name must not be empty.");
        this.queueName = queueName;
        this.tableIdentifier = tableIdentifier;
        this.connectionConfig =
                Preconditions.checkNotNull(
                        connectionConfig, "RabbitMQ connection configuration must not be null.");
        this.consistencyMode =
                Objects.isNull(consistencyMode) ? ConsistencyMode.AT_MOST_ONCE : consistencyMode;
    }

    @Override
    public DynamicTableSource copy() {
        final RabbitMQDynamicSource copy =
                new RabbitMQDynamicSource(
                        physicalDataType,
                        decodingFormat,
                        valueProjection,
                        queueName,
                        connectionConfig,
                        consistencyMode,
                        tableIdentifier);
        copy.producedDataType = producedDataType;
        copy.watermarkStrategy = watermarkStrategy;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "RabbitMQ table source";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final DeserializationSchema<RowData> keyDeserialization =
                createDeserialization(runtimeProviderContext, decodingFormat, valueProjection);

        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(runtimeProviderContext, decodingFormat, valueProjection);

        final TypeInformation<RowData> producedTypeInfo =
                runtimeProviderContext.createTypeInformation(producedDataType);

        final RabbitMQSource<RowData> rabbitMQSource =
                createRabbitMQSource(keyDeserialization, valueDeserialization, producedTypeInfo);

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                if (watermarkStrategy == null) {
                    watermarkStrategy = WatermarkStrategy.noWatermarks();
                }
                return execEnv.fromSource(
                        rabbitMQSource,
                        watermarkStrategy,
                        String.format("RabbitMQSource-%s", tableIdentifier));
            }

            @Override
            public boolean isBounded() {
                return rabbitMQSource.getBoundedness() == Boundedness.BOUNDED;
            }
        };
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy =
                watermarkStrategy == null ? WatermarkStrategy.noWatermarks() : watermarkStrategy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RabbitMQDynamicSource that = (RabbitMQDynamicSource) o;
        return Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(watermarkStrategy, that.watermarkStrategy)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(decodingFormat, that.decodingFormat)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(queueName, that.queueName)
                && Objects.equals(connectionConfig, that.connectionConfig)
                && Objects.equals(consistencyMode, that.consistencyMode)
                && Objects.equals(tableIdentifier, that.tableIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedDataType,
                watermarkStrategy,
                physicalDataType,
                decodingFormat,
                Arrays.hashCode(valueProjection),
                queueName,
                connectionConfig,
                consistencyMode,
                tableIdentifier);
    }

    private RabbitMQSource<RowData> createRabbitMQSource(
            DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {

        final DeserializationSchema<RowData> deserializationSchema =
                createRabbitMQDeserializationSchema(
                        keyDeserialization, valueDeserialization, producedTypeInfo);

        final RabbitMQSource.RabbitMQSourceBuilder<RowData> rabbitMQSourceBuilder =
                RabbitMQSource.<RowData>builder()
                        .setQueueName(queueName)
                        .setConsistencyMode(consistencyMode)
                        .setDeserializationSchema(deserializationSchema)
                        .setConnectionConfig(connectionConfig);

        return rabbitMQSourceBuilder.build();
    }

    private DeserializationSchema<RowData> createRabbitMQDeserializationSchema(
            DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {

        // adjust physical arity with value format's metadata
        final int adjustedPhysicalArity = DataType.getFieldDataTypes(producedDataType).size();

        // adjust value format projection to include value format's metadata columns at the end
        final int[] adjustedValueProjection =
                IntStream.concat(
                                IntStream.of(valueProjection),
                                IntStream.range(valueProjection.length, adjustedPhysicalArity))
                        .toArray();

        return new DynamicRabbitMQDeserializationSchema(
                adjustedPhysicalArity,
                keyDeserialization,
                valueDeserialization,
                adjustedValueProjection,
                producedTypeInfo);
    }

    private DeserializationSchema<RowData> createDeserialization(
            ScanContext runtimeProviderContext,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            int[] projection) {
        if (Objects.isNull(decodingFormat)) {
            return null;
        }
        DataType physicalFormatDataType = Projection.of(projection).project(this.physicalDataType);
        return decodingFormat.createRuntimeDecoder(runtimeProviderContext, physicalFormatDataType);
    }
}
