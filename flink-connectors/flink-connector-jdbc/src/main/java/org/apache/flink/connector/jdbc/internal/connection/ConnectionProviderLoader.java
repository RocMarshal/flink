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

package org.apache.flink.connector.jdbc.internal.connection;

import org.apache.flink.annotation.Internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Utility for working with {@link
 * org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider}.
 */
@Internal
public class ConnectionProviderLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionProviderLoader.class);

    private ConnectionProviderLoader() {}

    public static Optional<JdbcConnectionProviderFactory> load(String identifier) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        List<JdbcConnectionProviderFactory> foundFactories = discoverFactories(cl);

        if (foundFactories.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Could not find any JdbcConnectionProvider factories that implement '%s' in the classpath.",
                            JdbcConnectionProviderFactory.class.getName()));
        }

        final List<JdbcConnectionProviderFactory> matchingFactories =
                foundFactories.stream()
                        .filter(f -> Objects.equals(identifier, f.factoryIdentifier()))
                        .collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            return Optional.empty();
        }
        if (matchingFactories.size() > 1) {
            throw new IllegalStateException(
                    String.format(
                            "Multiple JdbcConnectionProvider factories can handle identifier '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            identifier,
                            JdbcConnectionProviderFactory.class.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return Optional.of(matchingFactories.get(0));
    }

    private static List<JdbcConnectionProviderFactory> discoverFactories(ClassLoader classLoader) {
        try {
            final List<JdbcConnectionProviderFactory> result = new LinkedList<>();
            ServiceLoader.load(JdbcConnectionProviderFactory.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for JdbcConnectionProvider factory.", e);
            throw new RuntimeException(
                    "Could not load service provider for JdbcConnectionProvider factory.", e);
        }
    }
}
