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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.flink.connector.rabbitmq2.table.DynamicTableUtils.getConnectionConfig;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.AUTOMATIC_RECOVERY;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.CONSISTENCY_MODE;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.NETWORK_RECOVERY_INTERVAL;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.PASSWORD;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.PREFETCH_COUNT;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.QUEUE;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.REQUESTED_CHANNEL_MAX;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.REQUESTED_FRAME_MAX;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.REQUESTED_HEARTBEAT;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.TOPOLOGY_RECOVERY;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.URI;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.USERNAME;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.VALUE_FORMAT;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.VIRTUAL_HOST;

/** Factory for RabbitMQDynamicTable . */
public class RabbitMQDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQDynamicTableFactory.class);

    public static final String IDENTIFIER = "rabbitmq";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);
        final ReadableConfig tableOptions = helper.getOptions();
        Preconditions.checkNotNull(tableOptions.get(QUEUE), "queue");
        final DataType physicalDataType = context.getPhysicalRowDataType();
        final int[] valueProjection =
                IntStream.range(
                                0,
                                LogicalTypeChecks.getFieldCount(physicalDataType.getLogicalType()))
                        .toArray();
        return new RabbitMQDynamicSource(
                physicalDataType,
                valueDecodingFormat,
                valueProjection,
                context.getCatalogTable()
                        .getOptions()
                        .getOrDefault(QUEUE.key(), QUEUE.defaultValue()),
                getConnectionConfig(context.getCatalogTable().getOptions()),
                helper.getOptions().get(CONSISTENCY_MODE),
                context.getObjectIdentifier().asSummaryString());
    }

    private static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(
                        () ->
                                helper.discoverDecodingFormat(
                                        DeserializationFormatFactory.class, VALUE_FORMAT));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<ConfigOption<?>>() {
            {
                add(QUEUE);
            }
        };
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<ConfigOption<?>>() {
            {
                add(VIRTUAL_HOST);
                add(USERNAME);
                add(PASSWORD);
                add(URI);
                add(CONSISTENCY_MODE);
                add(AUTOMATIC_RECOVERY);
                add(TOPOLOGY_RECOVERY);
                add(NETWORK_RECOVERY_INTERVAL);
                add(CONNECTION_TIMEOUT);
                add(REQUESTED_CHANNEL_MAX);
                add(REQUESTED_FRAME_MAX);
                add(REQUESTED_HEARTBEAT);
                add(PREFETCH_COUNT);
            }
        };
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                getValueEncodingFormat(helper);

        final DataType physicalDataType = context.getPhysicalRowDataType();

        final int[] valueProjection =
                IntStream.range(
                                0,
                                LogicalTypeChecks.getFieldCount(physicalDataType.getLogicalType()))
                        .toArray();

        return new RabbitMQDynamicSink(
                physicalDataType,
                physicalDataType,
                valueEncodingFormat,
                valueProjection,
                Preconditions.checkNotNull(context.getCatalogTable().getOptions().get(QUEUE.key())),
                getConnectionConfig(context.getCatalogTable().getOptions()),
                helper.getOptions().get(CONSISTENCY_MODE),
                null,
                null,
                null);
    }

    private EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalEncodingFormat(
                        SerializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(
                        () ->
                                helper.discoverEncodingFormat(
                                        SerializationFormatFactory.class, VALUE_FORMAT));
    }
}
