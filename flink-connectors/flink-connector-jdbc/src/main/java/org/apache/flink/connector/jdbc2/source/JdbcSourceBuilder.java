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

package org.apache.flink.connector.jdbc2.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc2.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc2.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc2.source.enumerator.JdbcSqlSplitEnumeratorBase;
import org.apache.flink.connector.jdbc2.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc2.source.split.JdbcSourceSplit;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.ResultSet;

import static org.apache.flink.connector.jdbc2.source.JdbcSourceOptions.AUTO_COMMIT;
import static org.apache.flink.connector.jdbc2.source.JdbcSourceOptions.READER_FETCH_BATCH_SIZE;
import static org.apache.flink.connector.jdbc2.source.JdbcSourceOptions.RESULTSET_CONCURRENCY;
import static org.apache.flink.connector.jdbc2.source.JdbcSourceOptions.RESULTSET_FETCH_SIZE;
import static org.apache.flink.connector.jdbc2.source.JdbcSourceOptions.RESULTSET_TYPE;

public class JdbcSourceBuilder<OUT> implements Serializable {

    public static final Logger LOG = LoggerFactory.getLogger(JdbcSourceBuilder.class);

    private final Configuration configuration;

    private int splitReaderFetchBatchSize;
    private int resultSetType;
    private int resultSetConcurrency;
    private int resultSetFetchSize;
    // Boolean to distinguish between default value and explicitly set autoCommit mode.
    private Boolean autoCommit;

    private DeliveryGuarantee deliveryGuarantee;

    private TypeInformation<OUT> typeInformation;

    private final JdbcConnectionOptions.JdbcConnectionOptionsBuilder connOptionsBuilder;
    private ContinuousEnumerationSettings continuousEnumerationSettings;
    private JdbcSqlSplitEnumeratorBase.Provider<JdbcSourceSplit> sqlSplitEnumeratorProvider;
    private ResultExtractor<OUT> resultExtractor;

    private JdbcConnectionProvider connectionProvider;

    public JdbcSourceBuilder() {
        this.configuration = new Configuration();
        this.connOptionsBuilder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        this.splitReaderFetchBatchSize = 1024;
        this.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        this.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        this.deliveryGuarantee = DeliveryGuarantee.NONE;
        // Boolean to distinguish between default value and explicitly set autoCommit mode.
        this.autoCommit = true;
    }

    public JdbcSourceBuilder<OUT> setSqlSplitEnumeratorProvider(
            JdbcSqlSplitEnumeratorBase.Provider<JdbcSourceSplit> sqlSplitEnumeratorProvider) {
        this.sqlSplitEnumeratorProvider =
                Preconditions.checkNotNull(
                        sqlSplitEnumeratorProvider, "sqlSplitEnumerator must not be null.");
        return this;
    }

    public JdbcSourceBuilder<OUT> setResultExtractor(ResultExtractor<OUT> resultExtractor) {
        this.resultExtractor =
                Preconditions.checkNotNull(resultExtractor, "resultExtractor must not be null.");
        return this;
    }

    public JdbcSourceBuilder<OUT> setUsername(String username) {
        connOptionsBuilder.withUsername(username);
        return this;
    }

    public JdbcSourceBuilder<OUT> setPassword(String password) {
        connOptionsBuilder.withPassword(password);
        return this;
    }

    public JdbcSourceBuilder<OUT> setDriverName(String driverName) {
        connOptionsBuilder.withDriverName(driverName);
        return this;
    }

    public JdbcSourceBuilder<OUT> setDBUrl(String dbURL) {
        connOptionsBuilder.withUrl(dbURL);
        return this;
    }

    // ------ Optional ------------------------------------------------------------------

    public JdbcSourceBuilder<OUT> setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = deliveryGuarantee;
        return this;
    }

    public JdbcSourceBuilder<OUT> setTypeInformation(TypeInformation<OUT> typeInformation) {
        this.typeInformation = typeInformation;
        return this;
    }

    public JdbcSourceBuilder<OUT> setSplitReaderFetchBatchSize(int splitReaderFetchBatchSize) {
        Preconditions.checkArgument(
                splitReaderFetchBatchSize > 0,
                String.format(
                        "splitReaderFetchBatchSize must be in range (0, %s]", Integer.MAX_VALUE));
        this.splitReaderFetchBatchSize = splitReaderFetchBatchSize;
        return this;
    }

    public JdbcSourceBuilder<OUT> setResultSetType(int resultSetType) {
        this.resultSetType = resultSetType;
        return this;
    }

    public JdbcSourceBuilder<OUT> setResultSetConcurrency(int resultSetConcurrency) {
        this.resultSetConcurrency = resultSetConcurrency;
        return this;
    }

    public JdbcSourceBuilder<OUT> setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    public JdbcSourceBuilder<OUT> setResultSetFetchSize(int resultSetFetchSize) {
        Preconditions.checkArgument(
                resultSetFetchSize == Integer.MIN_VALUE || resultSetFetchSize > 0,
                "Illegal value %s for fetchSize, has to be positive or Integer.MIN_VALUE.",
                resultSetFetchSize);
        this.resultSetFetchSize = resultSetFetchSize;
        return this;
    }

    public JdbcSourceBuilder<OUT> setContinuousEnumerationSettings(
            ContinuousEnumerationSettings continuousEnumerationSettings) {
        this.continuousEnumerationSettings =
                Preconditions.checkNotNull(continuousEnumerationSettings);
        return this;
    }

    public JdbcSourceBuilder<OUT> setConnectionProvider(JdbcConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
        return this;
    }

    public JdbcSource<OUT> build() {
        connectionProvider = new SimpleJdbcConnectionProvider(connOptionsBuilder.build());
        this.configuration.set(RESULTSET_FETCH_SIZE, resultSetFetchSize);
        this.configuration.set(RESULTSET_CONCURRENCY, resultSetConcurrency);
        this.configuration.set(RESULTSET_TYPE, resultSetType);
        this.configuration.set(READER_FETCH_BATCH_SIZE, splitReaderFetchBatchSize);
        this.configuration.set(AUTO_COMMIT, autoCommit);

        if (sqlSplitEnumeratorProvider == null) {
            throw new NullPointerException("No sqlSplitEnumeratorProvider supplied");
        }
        if (resultExtractor == null) {
            throw new NullPointerException("resultSet record extractor mustn't null");
        }
        return new JdbcSource<>(
                configuration,
                connectionProvider,
                sqlSplitEnumeratorProvider,
                resultExtractor,
                typeInformation,
                deliveryGuarantee,
                continuousEnumerationSettings);
    }
}
