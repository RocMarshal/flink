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

package org.apache.flink.connector.jdbc.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.source.JdbcSourceOptions;
import org.apache.flink.connector.jdbc.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Queue;

/** The {@link SplitReader} implementation for the jdbc source. */
@Internal
public class JdbcSourceSplitReader<T>
        implements SplitReader<RecordAndOffset<T>, JdbcSourceSplit>, ResultTypeQueryable<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceSplitReader.class);

    private final Configuration config;
    @Nullable private JdbcSourceSplit currentSplit;
    private final Queue<JdbcSourceSplit> splits;
    protected TypeInformation<T> typeInformation;
    protected JdbcConnectionProvider connectionProvider;
    protected transient Connection connection;
    protected transient PreparedStatement statement;
    protected transient ResultSet resultSet;

    private final ResultExtractor<T> resultExtractor;
    protected boolean hasNextRecordCurrentSplit;
    private final DeliveryGuarantee deliveryGuarantee;

    private final int splitReaderFetchBatchSize;

    protected int resultSetType;
    protected int resultSetConcurrency;
    protected int resultSetFetchSize;
    // Boolean to distinguish between default value and explicitly set autoCommit mode.
    protected Boolean autoCommit;

    private final SourceReaderContext context;

    public JdbcSourceSplitReader(
            SourceReaderContext context,
            Configuration config,
            TypeInformation<T> typeInformation,
            JdbcConnectionProvider connectionProvider,
            DeliveryGuarantee deliveryGuarantee,
            ResultExtractor<T> resultExtractor) {
        this.context = Preconditions.checkNotNull(context);
        this.config = Preconditions.checkNotNull(config);
        this.typeInformation = Preconditions.checkNotNull(typeInformation);
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.resultSetType = config.getInteger(JdbcSourceOptions.RESULTSET_TYPE);
        this.resultSetConcurrency = config.getInteger(JdbcSourceOptions.RESULTSET_CONCURRENCY);
        this.resultSetFetchSize = config.getInteger(JdbcSourceOptions.RESULTSET_FETCH_SIZE);
        this.autoCommit = config.getBoolean(JdbcSourceOptions.AUTO_COMMIT);
        this.deliveryGuarantee = Preconditions.checkNotNull(deliveryGuarantee);
        this.splits = new ArrayDeque<>();
        this.hasNextRecordCurrentSplit = false;
        this.currentSplit = null;
        int splitReaderFetchBatchSize =
                config.getInteger(JdbcSourceOptions.READER_FETCH_BATCH_SIZE);
        Preconditions.checkArgument(
                splitReaderFetchBatchSize > 0 && splitReaderFetchBatchSize < Integer.MAX_VALUE);
        this.splitReaderFetchBatchSize = splitReaderFetchBatchSize;
        this.resultExtractor = Preconditions.checkNotNull(resultExtractor);
    }

    @Override
    public RecordsWithSplitIds<RecordAndOffset<T>> fetch() throws IOException {

        checkSplitOrStartNext();

        if (!hasNextRecordCurrentSplit) {
            return finishSplit();
        }

        RecordsBySplits.Builder<RecordAndOffset<T>> eBuilder = new RecordsBySplits.Builder<>();
        Preconditions.checkState(currentSplit != null, "currentSplit");
        int batch = this.splitReaderFetchBatchSize;
        while (batch > 0 && hasNextRecordCurrentSplit) {
            try {
                T ele = resultExtractor.extract(resultSet);
                eBuilder.add(currentSplit, new RecordAndOffset<>(ele, resultSet.getRow()));
                batch--;
                hasNextRecordCurrentSplit = resultSet.next();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (!hasNextRecordCurrentSplit) {
            eBuilder.addFinishedSplit(currentSplit.splitId());
            closeResultSetAndStatement();
        }
        return eBuilder.build();
    }

    private RecordsWithSplitIds<RecordAndOffset<T>> finishSplit() {

        closeResultSetAndStatement();

        RecordsBySplits.Builder<RecordAndOffset<T>> builder = new RecordsBySplits.Builder<>();
        Preconditions.checkState(currentSplit != null, "currentSplit");
        builder.addFinishedSplit(currentSplit.splitId());
        currentSplit = null;
        return builder.build();
    }

    private void closeResultSetAndStatement() {
        try {
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            resultSet = null;
            statement = null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<JdbcSourceSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        LOG.debug("Handling split change {}", splitsChanges);
        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        closeResultSetAndStatement();
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        connection = null;
        currentSplit = null;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return typeInformation;
    }

    private void checkSplitOrStartNext() {

        try {
            if (hasNextRecordCurrentSplit && resultSet != null) {
                return;
            }

            final JdbcSourceSplit nextSplit = splits.poll();
            if (nextSplit == null) {
                throw new IOException("Cannot fetch from another split - no split remaining");
            }
            currentSplit = nextSplit;
            openResultSetForSplit(currentSplit);
        } catch (SQLException | ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void discardSplit(JdbcSourceSplit split) throws SQLException {
        if (split.getOffset() != 0) {
            hasNextRecordCurrentSplit = false;
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            resultSet = null;
            statement = null;
            currentSplit = null;
        }
    }

    private void reOpenConnectionIfNeeded() throws SQLException, ClassNotFoundException {
        connection = connectionProvider.getOrEstablishConnection();
        if (autoCommit != null) {
            connection.setAutoCommit(autoCommit);
        }
    }

    private void openResultSetForSplit(JdbcSourceSplit split)
            throws SQLException, ClassNotFoundException {
        if (deliveryGuarantee == DeliveryGuarantee.NONE && split.getOffset() != 0) {
            // DeliveryGuarantee.AT_MOST_ONCE
            discardSplit(split);
            return;
        }
        reOpenConnectionIfNeeded();
        statement =
                connection.prepareStatement(
                        split.getSqlTemplate(), resultSetType, resultSetConcurrency);
        if (split.getParameters() != null) {
            Object[] objs = split.getParameters();
            for (int i = 0; i < objs.length; i++) {
                statement.setObject(i + 1, objs[i]);
            }
        }
        statement.setFetchSize(resultSetFetchSize);
        resultSet = statement.executeQuery();
        // AT_LEAST_ONCE
        hasNextRecordCurrentSplit = resultSet.next();
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE && hasNextRecordCurrentSplit) {
            // DeliveryGuarantee.EXACTLY_ONCE
            resultSet.last();
            int last = resultSet.getRow();
            resultSet.absolute(1);
            Preconditions.checkState(currentSplit != null, "currentSplit");
            int resultSetOffset = currentSplit.getReaderPosition();
            if (resultSetOffset <= last) {
                resultSet.absolute(resultSetOffset);
            } else {
                hasNextRecordCurrentSplit = false;
                LOG.warn("The offset will not be set from splitState.");
            }
        }
    }
}
