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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.connector.rabbitmq2.common.ConsistencyMode;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;

/** RabbitMQ connection options. */
public class RabbitMQConnectorOptions {

    private RabbitMQConnectorOptions() {}

    // --------------------------------------------------------------------------------------------
    // RabbitMQ specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> QUEUE =
            ConfigOptions.key("queue")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The target RabbitMQ queue name to consume or produce.");

    public static final ConfigOption<ConsistencyMode> CONSISTENCY_MODE =
            ConfigOptions.key("consistency-mode")
                    .enumType(ConsistencyMode.class)
                    .defaultValue(ConsistencyMode.AT_MOST_ONCE)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "AT_MOST_ONCE: Messages are consumed by the output once or never;")
                                    .linebreak()
                                    .text(
                                            "AT_LEAST_ONCE: Messages are consumed by the output at least once;")
                                    .linebreak()
                                    .text(
                                            "EXACTLY_ONCE: Messages are consumed by the output exactly once.")
                                    .build());

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .defaultValue("localhost")
                    .withDescription(
                            Description.builder().text("The target RabbtMQ host.").build());

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(5762)
                    .withDescription(
                            Description.builder().text("The target RabbitMQ port.").build());

    public static final ConfigOption<String> VIRTUAL_HOST =
            ConfigOptions.key("virtual-host")
                    .stringType()
                    .defaultValue("/")
                    .withDescription(
                            Description.builder()
                                    .text("The target RabbitMQ virtual host.")
                                    .build());

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .defaultValue("guest")
                    .withDescription(
                            Description.builder().text("The target RabbitMQ username.").build());

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .defaultValue("guest")
                    .withDescription(
                            Description.builder()
                                    .text("The target RabbitMQ virtual password.")
                                    .build());

    public static final ConfigOption<String> URI =
            ConfigOptions.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder().text("The target RabbitMQ uri.").build());

    public static final ConfigOption<Boolean> AUTOMATIC_RECOVERY =
            ConfigOptions.key("automatic-recovery")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "It's used to mark whether client could recovery automatically.")
                                    .build());

    public static final ConfigOption<Boolean> TOPOLOGY_RECOVERY =
            ConfigOptions.key("topology-recovery")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "It's used to mark whether client topology could recovery automatically.")
                                    .build());

    public static final ConfigOption<Integer> NETWORK_RECOVERY_INTERVAL =
            ConfigOptions.key("network-recovery-interval")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The client of RabbitMQ network recovery interval in millisecond unit.")
                                    .build());

    public static final ConfigOption<Integer> CONNECTION_TIMEOUT =
            ConfigOptions.key("connection-timeout")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The client of RabbitMQ network connection timeout in millisecond unit.")
                                    .build());

    public static final ConfigOption<Integer> REQUESTED_CHANNEL_MAX =
            ConfigOptions.key("requested-channel-max")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("The max requested channel in RabbitMQ client.")
                                    .build());

    public static final ConfigOption<Integer> REQUESTED_FRAME_MAX =
            ConfigOptions.key("requested-frame-max")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("The max requested frame in RabbitMQ client.")
                                    .build());

    public static final ConfigOption<Integer> REQUESTED_HEARTBEAT =
            ConfigOptions.key("requested-heartbeat")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("The requested heartbeat in RabbitMQ client.")
                                    .build());

    // basicQos options for consumers
    public static final ConfigOption<Integer> PREFETCH_COUNT =
            ConfigOptions.key("prefetch-count")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("The prefetch-count in RabbitMQ client.")
                                    .build());

    public static final ConfigOption<String> VALUE_FORMAT =
            ConfigOptions.key("value" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding value data. "
                                    + "The identifier is used to discover a suitable format factory.");
}
