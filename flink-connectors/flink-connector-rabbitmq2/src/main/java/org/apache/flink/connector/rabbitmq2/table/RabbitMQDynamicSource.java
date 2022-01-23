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
import org.apache.flink.connector.rabbitmq2.source.reader.deserialization.RabbitMQDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.rabbitmq.client.Delivery;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.connector.rabbitmq2.table.DynamicRabbitMQDeserializationSchema.MetadataConverter;

/** A RabbitMQ {@link ScanTableSource}. */
@Internal
public class RabbitMQDynamicSource
        implements ScanTableSource, SupportsReadingMetadata, SupportsWatermarkPushDown {

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    private DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    private List<String> metadataKeys;

    /** Watermark strategy that is used to generate per-partition watermark. */
    private @Nullable WatermarkStrategy<RowData> watermarkStrategy;

    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    private static final String VALUE_METADATA_PREFIX = "value.";

    /** Data type to configure the formats. */
    private final DataType physicalDataType;

    /** Optional format for decoding keys from RabbitMQ. */
    protected final @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat;

    /** Format for decoding values from RabbitMQ. */
    protected final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    /** Indices that determine the key fields and the target position in the produced row. */
    protected final int[] keyProjection;

    /** Indices that determine the value fields and the target position in the produced row. */
    protected final int[] valueProjection;

    /** Prefix that needs to be removed from fields when constructing the physical data type. */
    protected final @Nullable String keyPrefix;

    // --------------------------------------------------------------------------------------------
    // RabbitMQ-specific attributes
    // --------------------------------------------------------------------------------------------

    /** The RabbitMQ queue name to consume. */
    private final String queueName;

    /** The configuration for the RabbitMQ connection. */
    private final RabbitMQConnectionConfig connectionConfig;

    /** The consistency mode for the source. */
    private final ConsistencyMode consistencyMode;

    /** Flag to determine source mode. In upsert mode, it will keep the tombstone message. * */
    protected final boolean upsertMode;

    protected final String tableIdentifier;

    public RabbitMQDynamicSource(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            String queueName,
            RabbitMQConnectionConfig connectionConfig,
            ConsistencyMode consistencyMode,
            boolean upsertMode,
            String tableIdentifier) {
        // Format attributes
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.keyDecodingFormat = keyDecodingFormat;
        this.valueDecodingFormat =
                Preconditions.checkNotNull(
                        valueDecodingFormat, "Value decoding format must not be null.");
        this.keyProjection =
                Preconditions.checkNotNull(keyProjection, "Key projection must not be null.");
        this.valueProjection =
                Preconditions.checkNotNull(valueProjection, "Value projection must not be null.");
        this.keyPrefix = keyPrefix;

        // Mutable attributes
        this.producedDataType = physicalDataType;
        this.metadataKeys = Collections.emptyList();
        this.watermarkStrategy = null;

        // RabbitMQ0-specific attributes
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(queueName),
                "RabbitMQ queue name must not be empty.");
        this.queueName = queueName;
        this.upsertMode = upsertMode;
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
                        keyDecodingFormat,
                        valueDecodingFormat,
                        keyProjection,
                        valueProjection,
                        keyPrefix,
                        queueName,
                        connectionConfig,
                        consistencyMode,
                        upsertMode,
                        tableIdentifier);
        copy.producedDataType = producedDataType;
        copy.metadataKeys = metadataKeys;
        copy.watermarkStrategy = watermarkStrategy;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "RabbitMQ table source";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return valueDecodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final DeserializationSchema<RowData> keyDeserialization =
                createDeserialization(
                        runtimeProviderContext, keyDecodingFormat, keyProjection, keyPrefix);

        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(
                        runtimeProviderContext, valueDecodingFormat, valueProjection, null);

        final TypeInformation<RowData> producedTypeInfo =
                runtimeProviderContext.createTypeInformation(producedDataType);

        final RabbitMQSource<RowData> rabbitMQSource =
                createRabbitMQSource(keyDeserialization, valueDeserialization, producedTypeInfo);

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
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
    public Map<String, DataType> listReadableMetadata() {
        return null;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {}

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
                && Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(watermarkStrategy, that.watermarkStrategy)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(keyDecodingFormat, that.keyDecodingFormat)
                && Objects.equals(valueDecodingFormat, that.valueDecodingFormat)
                && Arrays.equals(keyProjection, that.keyProjection)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(keyPrefix, that.keyPrefix)
                && Objects.equals(queueName, that.queueName)
                && Objects.equals(connectionConfig, that.connectionConfig)
                && Objects.equals(consistencyMode, that.consistencyMode)
                && Objects.equals(upsertMode, that.upsertMode)
                && Objects.equals(tableIdentifier, that.tableIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedDataType,
                metadataKeys,
                watermarkStrategy,
                physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                Arrays.hashCode(keyProjection),
                Arrays.hashCode(valueProjection),
                keyPrefix,
                queueName,
                connectionConfig,
                consistencyMode,
                upsertMode,
                tableIdentifier);
    }

    private RabbitMQSource<RowData> createRabbitMQSource(
            DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {

        final RabbitMQDeserializationSchema<RowData> deserializationSchema =
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

    private RabbitMQDeserializationSchema<RowData> createRabbitMQDeserializationSchema(
            DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {
        final MetadataConverter[] metadataConverters =
                metadataKeys.stream()
                        .map(
                                k ->
                                        Stream.of(ReadableMetadata.values())
                                                .filter(rm -> rm.key.equals(k))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new))
                        .map(m -> m.converter)
                        .toArray(MetadataConverter[]::new);

        // check if connector metadata is used at all
        final boolean hasMetadata = metadataKeys.size() > 0;

        // adjust physical arity with value format's metadata
        final int adjustedPhysicalArity =
                DataType.getFieldDataTypes(producedDataType).size() - metadataKeys.size();

        // adjust value format projection to include value format's metadata columns at the end
        final int[] adjustedValueProjection =
                IntStream.concat(
                                IntStream.of(valueProjection),
                                IntStream.range(
                                        keyProjection.length + valueProjection.length,
                                        adjustedPhysicalArity))
                        .toArray();

        return new DynamicRabbitMQDeserializationSchema(
                adjustedPhysicalArity,
                keyDeserialization,
                keyProjection,
                valueDeserialization,
                adjustedValueProjection,
                hasMetadata,
                metadataConverters,
                producedTypeInfo,
                upsertMode);
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum ReadableMetadata {
        MESSAGE_ID(
                "message.id",
                DataTypes.STRING().notNull(),
                new MetadataConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(Delivery delivery) {
                        if (isPropertiesEmpty(delivery)
                                && delivery.getProperties().getMessageId() != null) {
                            return StringData.fromString(delivery.getProperties().getMessageId());
                        }
                        return StringData.fromString("-1");
                    }
                }),

        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                new MetadataConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(Delivery delivery) {
                        if (isPropertiesEmpty(delivery)
                                && delivery.getProperties().getTimestamp() != null) {
                            return TimestampData.fromEpochMillis(
                                    delivery.getProperties().getTimestamp().getTime());
                        }
                        return TimestampData.fromEpochMillis(System.currentTimeMillis());
                    }
                });

        final String key;

        final DataType dataType;

        final MetadataConverter converter;

        private static boolean isPropertiesEmpty(Delivery delivery) {
            return delivery != null && delivery.getProperties() != null;
        }

        ReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }

    private DeserializationSchema<RowData> createDeserialization(
            ScanContext runtimeProviderContext,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            int[] projection,
            String prefix) {
        if (Objects.isNull(decodingFormat)) {
            return null;
        }
        DataType physicalFormatDataType = Projection.of(projection).project(this.physicalDataType);
        if (Objects.nonNull(prefix)) {
            physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
        }
        return decodingFormat.createRuntimeDecoder(runtimeProviderContext, physicalFormatDataType);
    }
}
