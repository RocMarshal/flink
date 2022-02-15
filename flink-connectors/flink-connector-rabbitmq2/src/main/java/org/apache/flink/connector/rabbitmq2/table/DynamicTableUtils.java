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

import org.apache.flink.connector.rabbitmq2.common.RabbitMQConnectionConfig;
import org.apache.flink.util.StringUtils;

import java.util.Map;

import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.AUTOMATIC_RECOVERY;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.HOST;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.NETWORK_RECOVERY_INTERVAL;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.PASSWORD;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.PORT;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.PREFETCH_COUNT;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.REQUESTED_CHANNEL_MAX;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.REQUESTED_FRAME_MAX;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.REQUESTED_HEARTBEAT;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.TOPOLOGY_RECOVERY;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.URI;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.USERNAME;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.VIRTUAL_HOST;

/** Utils for RabbitMQ Dynamic Table. */
public class DynamicTableUtils {

    private DynamicTableUtils() {}

    public static RabbitMQConnectionConfig getConnectionConfig(Map<String, String> tableOptions) {
        RabbitMQConnectionConfig.Builder builder =
                new RabbitMQConnectionConfig.Builder()
                        .setHost(tableOptions.getOrDefault(HOST.key(), HOST.defaultValue()))
                        .setPort(
                                Integer.parseInt(
                                        tableOptions.getOrDefault(
                                                PORT.key(), PORT.defaultValue().toString())))
                        .setVirtualHost(
                                tableOptions.getOrDefault(
                                        VIRTUAL_HOST.key(), VIRTUAL_HOST.defaultValue()))
                        .setUserName(
                                tableOptions.getOrDefault(USERNAME.key(), USERNAME.defaultValue()))
                        .setPassword(
                                tableOptions.getOrDefault(PASSWORD.key(), PASSWORD.defaultValue()))
                        .setUri(tableOptions.get(URI.key()));
        String recoverInterval = tableOptions.get(NETWORK_RECOVERY_INTERVAL.key());
        if (!StringUtils.isNullOrWhitespaceOnly(recoverInterval)) {
            builder.setNetworkRecoveryInterval(Integer.parseInt(recoverInterval));
        }
        String automaticRecovery = tableOptions.get(AUTOMATIC_RECOVERY.key());
        if (!StringUtils.isNullOrWhitespaceOnly(automaticRecovery)) {
            builder.setAutomaticRecovery(Boolean.parseBoolean(automaticRecovery));
        }
        String topologyRecoveryEnabled = tableOptions.get(TOPOLOGY_RECOVERY.key());
        if (!StringUtils.isNullOrWhitespaceOnly(topologyRecoveryEnabled)) {
            builder.setTopologyRecoveryEnabled(Boolean.parseBoolean(topologyRecoveryEnabled));
        }
        String connectionTimeout = tableOptions.get(CONNECTION_TIMEOUT.key());
        if (!StringUtils.isNullOrWhitespaceOnly(connectionTimeout)) {
            builder.setConnectionTimeout(Integer.parseInt(connectionTimeout));
        }
        String requestedChannelMax = tableOptions.get(REQUESTED_CHANNEL_MAX.key());
        if (!StringUtils.isNullOrWhitespaceOnly(requestedChannelMax)) {
            builder.setConnectionTimeout(Integer.parseInt(requestedChannelMax));
        }
        String requestedFrameMax = tableOptions.get(REQUESTED_FRAME_MAX.key());
        if (!StringUtils.isNullOrWhitespaceOnly(requestedFrameMax)) {
            builder.setConnectionTimeout(Integer.parseInt(requestedFrameMax));
        }
        String requestedHeartbeat = tableOptions.get(REQUESTED_HEARTBEAT.key());
        if (!StringUtils.isNullOrWhitespaceOnly(requestedHeartbeat)) {
            builder.setConnectionTimeout(Integer.parseInt(requestedHeartbeat));
        }
        String prefetchCount = tableOptions.get(PREFETCH_COUNT.key());
        if (!StringUtils.isNullOrWhitespaceOnly(prefetchCount)) {
            builder.setConnectionTimeout(Integer.parseInt(prefetchCount));
        }
        return builder.build();
    }
}
