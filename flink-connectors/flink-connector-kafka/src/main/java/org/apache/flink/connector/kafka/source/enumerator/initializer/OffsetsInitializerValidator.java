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

package org.apache.flink.connector.kafka.source.enumerator.initializer;

import org.apache.flink.annotation.Internal;

import java.util.Properties;
/**
 * 校验org.apache.flink.connector.kafka.source.KafkaSource 位点初始化器的校验器
 */

/**
 * Interface for validating {@link OffsetsInitializer} with properties from {@link
 * org.apache.flink.connector.kafka.source.KafkaSource}.
 */
@Internal
public interface OffsetsInitializerValidator {

    /**
     * Validate offsets initializer with properties of Kafka source.
     *
     * @param kafkaSourceProperties Properties of Kafka source
     * @throws IllegalStateException if validation fails
     */
    void validate(Properties kafkaSourceProperties) throws IllegalStateException;
}
