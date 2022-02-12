/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SerializableSupplier;

import javax.annotation.Nullable;
import javax.sql.XADataSource;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/** JDBC connection options. */
@PublicEvolving
public class JdbcConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String url;
    @Nullable protected final String driverName;
    protected final int connectionCheckTimeoutSeconds;
    @Nullable protected final String username;
    @Nullable protected final String password;
    @Nullable protected final Properties extendProps;
    @Nullable protected Boolean autoCommit;

    protected final SerializableSupplier<XADataSource> xaDataSourceSupplier;

    public JdbcConnectorOptions convertJdbcConnectorOptions() {
        throw new UnsupportedOperationException();
    }

    public SerializableSupplier<XADataSource> getXaDatasourceSupplier() {
        return this.xaDataSourceSupplier;
    }

    public Boolean getAutoCommit() {
        return autoCommit;
    }

    protected JdbcConnectionOptions(
            String url,
            @Nullable String driverName,
            @Nullable String username,
            @Nullable String password,
            int connectionCheckTimeoutSeconds,
            @Nullable Boolean autoCommit,
            @Nullable Properties extendProps,
            @Nullable SerializableSupplier<XADataSource> xaDataSourceSupplier) {
        Preconditions.checkArgument(connectionCheckTimeoutSeconds > 0);
        if (xaDataSourceSupplier == null) {
            this.url = Preconditions.checkNotNull(url, "jdbc url is empty");
        } else {
            this.url = null;
        }

        this.driverName = driverName;
        this.username = username;
        this.password = password;
        this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
        this.autoCommit = autoCommit;
        this.extendProps = extendProps;
        this.xaDataSourceSupplier = xaDataSourceSupplier;
    }

    public void checkSemantic(DeliveryGuarantee deliveryGuarantee) {
        if (DeliveryGuarantee.EXACTLY_ONCE == deliveryGuarantee) {
            Preconditions.checkArgument(
                    Objects.nonNull(xaDataSourceSupplier), "Must give a xa datasource supplier.");
        }
    }

    public Properties getExtendProps() {
        return extendProps == null ? new Properties() : extendProps;
    }

    public String getDbURL() {
        return url;
    }

    @Nullable
    public String getDriverName() {
        return driverName;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }

    public int getConnectionCheckTimeoutSeconds() {
        return connectionCheckTimeoutSeconds;
    }

    /** Builder for {@link JdbcConnectionOptions}. */
    public static class JdbcConnectionOptionsBuilder {
        private String url;
        private String driverName;
        private String username;
        private String password;
        private int connectionCheckTimeoutSeconds = 60;

        private Boolean autoCommit;

        protected Properties extendProps;

        protected SerializableSupplier<XADataSource> xaDatasourceSupplier;

        public JdbcConnectionOptionsBuilder withUrl(String url) {
            this.url = url;
            return this;
        }

        public JdbcConnectionOptionsBuilder withAutoCommit(Boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public JdbcConnectionOptionsBuilder withExtendProps(Properties extendProps) {
            this.extendProps = Preconditions.checkNotNull(extendProps, "extendProps");
            return this;
        }

        public JdbcConnectionOptionsBuilder withDriverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        public JdbcConnectionOptionsBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public JdbcConnectionOptionsBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        public JdbcConnectionOptionsBuilder withXaDatasourceSupplier(
                SerializableSupplier<XADataSource> xaDatasourceSupplier) {
            this.xaDatasourceSupplier = xaDatasourceSupplier;
            return this;
        }

        /**
         * Set the maximum timeout between retries, default is 60 seconds.
         *
         * @param connectionCheckTimeoutSeconds the timeout seconds, shouldn't smaller than 1
         *     second.
         */
        public JdbcConnectionOptionsBuilder withConnectionCheckTimeoutSeconds(
                int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        public JdbcConnectionOptions build() {
            return new JdbcConnectionOptions(
                    url,
                    driverName,
                    username,
                    password,
                    connectionCheckTimeoutSeconds,
                    autoCommit,
                    extendProps,
                    xaDatasourceSupplier);
        }
    }
}
