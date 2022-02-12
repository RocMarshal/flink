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

package org.apache.flink.connector.jdbc.sink.writer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcDataTestBase;
import org.apache.flink.connector.jdbc.JdbcDataTestBaseJUnit5;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcTestFixture;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link JdbcTableUpsertWriter}. */
public class JdbcTableUpsertWriterTest extends JdbcDataTestBaseJUnit5 {

    private JdbcTableUpsertWriter jdbcTableUpsertWriter;
    private String[] fieldNames;
    private String[] keyFields;

    @BeforeEach
    public void setup() {
        fieldNames = new String[] {"id", "title", "author", "price", "qty"};
        keyFields = new String[] {"id"};
    }

    /**
     * Test that the delete executor in {@link JdbcTableUpsertWriter} is updated when {@link
     * JdbcGenericWriter#attemptFlush()} fails.
     */
    @Test
    public void testDeleteExecutorUpdatedOnReconnect() throws Exception {
        // first fail flush from the main executor
        boolean[] exceptionThrown = {false};
        // then record whether the delete executor was updated
        // and check it on the next flush attempt
        boolean[] deleteExecutorPrepared = {false};
        boolean[] deleteExecuted = {false};
        jdbcTableUpsertWriter =
                new JdbcTableUpsertWriter(
                        mockContext,
                        JdbcWriterState.empty(),
                        new SimpleJdbcConnectionProvider(
                                JdbcConnectorOptions.builder()
                                        .setDBUrl(getDbMetadata().getUrl())
                                        .setTableName(OUTPUT_TABLE)
                                        .build()) {
                            @Override
                            public boolean isConnectionValid() throws SQLException {
                                return false; // trigger reconnect and re-prepare on flush failure
                            }
                        },
                        JdbcWriterConfig.builder()
                                .setJdbcExecutionOptions(
                                        JdbcExecutionOptions.builder()
                                                .withMaxRetries(1)
                                                .withBatchIntervalMs(
                                                        Long.MAX_VALUE) // disable periodic flush
                                                .build())
                                .build(),
                        (StatementExecutorFactory<JdbcBatchStatementExecutor<Row>>)
                                initContext ->
                                        new JdbcBatchStatementExecutor<Row>() {

                                            @Override
                                            public void executeBatch() throws SQLException {
                                                if (!exceptionThrown[0]) {
                                                    exceptionThrown[0] = true;
                                                    throw new SQLException();
                                                }
                                            }

                                            @Override
                                            public void prepareStatements(Connection connection) {}

                                            @Override
                                            public void addToBatch(Row record) {}

                                            @Override
                                            public void closeStatements() {}
                                        },
                        (StatementExecutorFactory<JdbcBatchStatementExecutor<Row>>)
                                initContext ->
                                        new JdbcBatchStatementExecutor<Row>() {
                                            @Override
                                            public void prepareStatements(Connection connection) {
                                                if (exceptionThrown[0]) {
                                                    deleteExecutorPrepared[0] = true;
                                                }
                                            }

                                            @Override
                                            public void addToBatch(Row record) {}

                                            @Override
                                            public void executeBatch() {
                                                deleteExecuted[0] = true;
                                            }

                                            @Override
                                            public void closeStatements() {}
                                        },
                        (RecordExtractor<Tuple2<Boolean, Row>, Row>)
                                booleanRowTuple2 -> booleanRowTuple2.f1);

        jdbcTableUpsertWriter.write(
                Tuple2.of(false /* false = delete*/, toRow(TEST_DATA[0])), mockSinkWriterContext);
        jdbcTableUpsertWriter.flush(false);

        assertThat(deleteExecuted[0]).as("Delete should be executed").isTrue();
        assertThat(deleteExecutorPrepared[0])
                .as("Delete executor should be prepared" + exceptionThrown[0])
                .isTrue();
    }

    @Test
    public void testJdbcOutputFormat() throws Exception {
        JdbcConnectorOptions options =
                JdbcConnectorOptions.builder()
                        .setDBUrl(getDbMetadata().getUrl())
                        .setTableName(OUTPUT_TABLE)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(options.getTableName())
                        .withDialect(options.getDialect())
                        .withFieldNames(fieldNames)
                        .withKeyFields(keyFields)
                        .build();
        jdbcTableUpsertWriter =
                new JdbcTableUpsertWriter(
                        mockContext,
                        JdbcWriterState.empty(),
                        new SimpleJdbcConnectionProvider(options),
                        JdbcWriterConfig.builder()
                                .setJdbcExecutionOptions(JdbcExecutionOptions.defaults())
                                .build(),
                        dmlOptions,
                        tuple2 -> tuple2.f1);

        for (JdbcTestFixture.TestEntry entry : TEST_DATA) {
            jdbcTableUpsertWriter.write(Tuple2.of(true, toRow(entry)), mockSinkWriterContext);
        }
        jdbcTableUpsertWriter.flush(false);
        check(Arrays.stream(TEST_DATA).map(JdbcDataTestBase::toRow).toArray(Row[]::new));

        // override
        for (JdbcTestFixture.TestEntry entry : TEST_DATA) {
            jdbcTableUpsertWriter.write(Tuple2.of(true, toRow(entry)), mockSinkWriterContext);
        }
        jdbcTableUpsertWriter.flush(false);
        check(Arrays.stream(TEST_DATA).map(JdbcDataTestBase::toRow).toArray(Row[]::new));

        // delete
        for (int i = 0; i < TEST_DATA.length / 2; i++) {
            jdbcTableUpsertWriter.write(
                    Tuple2.of(false, toRow(TEST_DATA[i])), mockSinkWriterContext);
        }
        Row[] expected = new Row[TEST_DATA.length - TEST_DATA.length / 2];
        for (int i = TEST_DATA.length / 2; i < TEST_DATA.length; i++) {
            expected[i - TEST_DATA.length / 2] = toRow(TEST_DATA[i]);
        }
        jdbcTableUpsertWriter.flush(false);
        check(expected);
    }

    private void check(Row[] rows) throws SQLException {
        check(rows, getDbMetadata().getUrl(), OUTPUT_TABLE, fieldNames);
    }

    public static void check(Row[] rows, String url, String table, String[] fields)
            throws SQLException {
        try (Connection dbConn = DriverManager.getConnection(url);
                PreparedStatement statement = dbConn.prepareStatement("select * from " + table);
                ResultSet resultSet = statement.executeQuery()) {
            List<String> results = new ArrayList<>();
            while (resultSet.next()) {
                Row row = new Row(fields.length);
                for (int i = 0; i < fields.length; i++) {
                    row.setField(i, resultSet.getObject(fields[i]));
                }
                results.add(row.toString());
            }
            String[] sortedExpect = Arrays.stream(rows).map(Row::toString).toArray(String[]::new);
            String[] sortedResult = results.toArray(new String[0]);
            Arrays.sort(sortedExpect);
            Arrays.sort(sortedResult);
            assertThat(sortedResult).isEqualTo(sortedExpect);
        }
    }

    @AfterEach
    public void clearOutputTable() throws Exception {
        if (jdbcTableUpsertWriter != null) {
            jdbcTableUpsertWriter.close();
        }
        jdbcTableUpsertWriter = null;
        Class.forName(getDbMetadata().getDriverClass());
        try (Connection conn = DriverManager.getConnection(getDbMetadata().getUrl());
                Statement stat = conn.createStatement()) {
            stat.execute("DELETE FROM " + OUTPUT_TABLE);
        }
    }
}
