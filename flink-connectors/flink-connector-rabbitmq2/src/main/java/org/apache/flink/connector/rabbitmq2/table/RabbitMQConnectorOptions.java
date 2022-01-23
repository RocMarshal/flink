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

import java.util.List;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;


public class RabbitMQConnectorOptions {

    private RabbitMQConnectorOptions() {}

    // --------------------------------------------------------------------------------------------
    // Format options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> KEY_FORMAT =
            ConfigOptions.key("key" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding key data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<String> VALUE_FORMAT =
            ConfigOptions.key("value" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding value data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<List<String>> KEY_FIELDS =
            ConfigOptions.key("key.fields")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription(
                            "Defines an explicit list of physical columns from the table schema "
                                    + "that configure the data type for the key format. By default, this list is "
                                    + "empty and thus a key is undefined.");

    public static final ConfigOption<ValueFieldsStrategy> VALUE_FIELDS_INCLUDE =
            ConfigOptions.key("value.fields-include")
                    .enumType(ValueFieldsStrategy.class)
                    .defaultValue(ValueFieldsStrategy.ALL)
                    .withDescription(
                            String.format(
                                    "Defines a strategy how to deal with key columns in the data type "
                                            + "of the value format. By default, '%s' physical columns of the table schema "
                                            + "will be included in the value format which means that the key columns "
                                            + "appear in the data type for both the key and value format.",
                                    ValueFieldsStrategy.ALL));

    public static final ConfigOption<String> KEY_FIELDS_PREFIX =
            ConfigOptions.key("key.fields-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines a custom prefix for all fields of the key format to avoid "
                                                    + "name clashes with fields of the value format. "
                                                    + "By default, the prefix is empty.")
                                    .linebreak()
                                    .text(
                                            String.format(
                                                    "If a custom prefix is defined, both the table schema and '%s' will work with prefixed names.",
                                                    KEY_FIELDS.key()))
                                    .linebreak()
                                    .text(
                                            "When constructing the data type of the key format, the prefix "
                                                    + "will be removed and the non-prefixed names will be used within the key format.")
                                    .linebreak()
                                    .text(
                                            String.format(
                                                    "Please note that this option requires that '%s' must be '%s'.",
                                                    VALUE_FIELDS_INCLUDE.key(),
                                                    ValueFieldsStrategy.EXCEPT_KEY))
                                    .build());

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
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "It's used to mark whether client could recovery automatically.")
                                    .build());

    public static final ConfigOption<Boolean> TOPOLOGY_RECOVERY =
            ConfigOptions.key("topology-recovery")
                    .booleanType()
                    .defaultValue(true)
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

    /** Strategies to derive the data type of a value format by considering a key format. */
    public enum ValueFieldsStrategy {
        ALL,
        EXCEPT_KEY
    }
}
