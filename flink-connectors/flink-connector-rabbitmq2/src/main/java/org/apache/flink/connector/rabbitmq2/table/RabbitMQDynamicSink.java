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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.rabbitmq2.common.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSink;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.common.SerializableReturnListener;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A RabbitMQ {@link DynamicTableSink}. */
@Internal
public class RabbitMQDynamicSink implements DynamicTableSink {

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    // protected List<String> metadataKeys;

    /** Data type of consumed data type. */
    protected DataType consumedDataType;

    /** Data type to configure the formats. */
    protected final DataType physicalDataType;

    /** Format for encoding values to RabbitMQ. */
    protected final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;

    @Nullable protected RabbitMQSinkPublishOptions<RowData> publishOptions;
    @Nullable protected SerializableReturnListener serializableReturnListener;

    /** Indices that determine the value fields and the source position in the consumed row. */
    protected final int[] valueProjection;

    // --------------------------------------------------------------------------------------------
    // RabbitMQ-specific attributes
    // --------------------------------------------------------------------------------------------

    /** The defined delivery guarantee. */
    private final ConsistencyMode consistencyMode;

    /** The rabbitMQ queue to write to. */
    protected final String queue;

    /** Properties for the RabbitMQ producer. */
    protected final RabbitMQConnectionConfig config;

    /** Parallelism of the physical RabbitMQ producer. * */
    protected final @Nullable Integer parallelism;

    public RabbitMQDynamicSink(
            DataType consumedDataType,
            DataType physicalDataType,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] valueProjection,
            String queue,
            RabbitMQConnectionConfig config,
            ConsistencyMode consistencyMode,
            @Nullable Integer parallelism,
            @Nullable RabbitMQSinkPublishOptions<RowData> publishOptions,
            @Nullable SerializableReturnListener returnListener) {
        // Format attributes
        this.consumedDataType =
                checkNotNull(consumedDataType, "Consumed data type must not be null.");
        this.physicalDataType =
                checkNotNull(physicalDataType, "Physical data type must not be null.");
        this.valueEncodingFormat =
                checkNotNull(valueEncodingFormat, "Value encoding format must not be null.");
        this.valueProjection = checkNotNull(valueProjection, "Value projection must not be null.");
        // Mutable attributes
        // this.metadataKeys = Collections.emptyList();

        this.queue = checkNotNull(queue, "Queue must not be null.");
        this.config = checkNotNull(config, "Config must not be null.");
        this.consistencyMode = checkNotNull(consistencyMode, "ConsistencyMode must not be null.");
        this.parallelism = parallelism;
        this.publishOptions = publishOptions;
        this.serializableReturnListener = returnListener;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return valueEncodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> valueSerialization =
                createSerialization(context, valueEncodingFormat, valueProjection);
        final RabbitMQSink.RabbitMQSinkBuilder<RowData> sinkBuilder = RabbitMQSink.builder();
        final RabbitMQSink<RowData> rabbitMQSink =
                sinkBuilder
                        .setConsistencyMode(consistencyMode)
                        .setConnectionConfig(config)
                        .setQueueName(queue)
                        .setSerializationSchema(valueSerialization)
                        .setPublishOptions(publishOptions)
                        .setReturnListener(serializableReturnListener)
                        .build();
        return SinkProvider.of(rabbitMQSink, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        final RabbitMQDynamicSink copy =
                new RabbitMQDynamicSink(
                        consumedDataType,
                        physicalDataType,
                        valueEncodingFormat,
                        valueProjection,
                        queue,
                        config,
                        consistencyMode,
                        parallelism,
                        publishOptions,
                        serializableReturnListener);
        // copy.metadataKeys = metadataKeys;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "RabbitMQ table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RabbitMQDynamicSink that = (RabbitMQDynamicSink) o;
        return // Objects.equals(metadataKeys, that.metadataKeys)
        Objects.equals(consumedDataType, that.consumedDataType)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(valueEncodingFormat, that.valueEncodingFormat)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(queue, that.queue)
                && Objects.equals(config, that.config)
                && Objects.equals(consistencyMode, that.consistencyMode)
                && Objects.equals(parallelism, that.parallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                // metadataKeys,
                consumedDataType,
                physicalDataType,
                valueEncodingFormat,
                Arrays.hashCode(valueProjection),
                queue,
                config,
                consistencyMode,
                parallelism);
    }

    private @Nullable SerializationSchema<RowData> createSerialization(
            DynamicTableSink.Context context,
            @Nullable EncodingFormat<SerializationSchema<RowData>> format,
            int[] projection) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType = Projection.of(projection).project(this.physicalDataType);
        return format.createRuntimeEncoder(context, physicalFormatDataType);
    }
}
