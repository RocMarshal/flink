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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcDataTestBaseJUnit5;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcTestFixture;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INSERT_TEMPLATE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE_2;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE_3;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS_2;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS_3;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for the {@link org.apache.flink.connector.jdbc.sink.writer.JdbcRowWriter}. */
public class JdbcRowWriterTest extends JdbcDataTestBaseJUnit5 {

    private JdbcRowWriter jdbcRowWriter;

    @AfterEach
    public void tearDown() throws Exception {
        if (jdbcRowWriter != null) {
            jdbcRowWriter.close();
        }
        jdbcRowWriter = null;
    }

    @AfterEach
    public void clearOutputTable() throws Exception {
        Class.forName(DERBY_EBOOKSHOP_DB.getDriverClass());
        try (Connection conn = DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl());
                Statement stat = conn.createStatement()) {
            stat.execute("DELETE FROM " + OUTPUT_TABLE);
        }
    }

    @Test
    public void testInvalidDriver() {
        //        String expectedMsg = "unable to open JDBC writer";

        assertThatThrownBy(
                        () -> {
                            jdbcRowWriter =
                                    new JdbcRowWriter(
                                            mockContext,
                                            null,
                                            String.format(INSERT_TEMPLATE, OUTPUT_TABLE),
                                            JdbcWriterState.empty(),
                                            new SimpleJdbcConnectionProvider(
                                                    new JdbcConnectionOptions
                                                                    .JdbcConnectionOptionsBuilder()
                                                            .withUrl(DERBY_EBOOKSHOP_DB.getUrl())
                                                            .withDriverName(
                                                                    "org.apache.derby.jdbc.idontexist")
                                                            .build()),
                                            JdbcWriterConfig.builder().build(),
                                            RecordExtractor.identity()) {};
                            jdbcRowWriter.close();
                        })
                .hasRootCauseInstanceOf(ClassNotFoundException.class);
    }

    @Test
    public void testInvalidURL() {
        String expectedMsg = "No suitable driver found for jdbc:der:iamanerror:mory:ebookshop";
        setMockContextObjectReusable(true);
        assertThatThrownBy(
                        () -> {
                            jdbcRowWriter =
                                    new JdbcRowWriter(
                                            mockContext,
                                            null,
                                            String.format(INSERT_TEMPLATE, OUTPUT_TABLE),
                                            JdbcWriterState.empty(),
                                            new SimpleJdbcConnectionProvider(
                                                    new JdbcConnectionOptions
                                                                    .JdbcConnectionOptionsBuilder()
                                                            .withUrl(
                                                                    "jdbc:der:iamanerror:mory:ebookshop")
                                                            .withDriverName(
                                                                    DERBY_EBOOKSHOP_DB
                                                                            .getDriverClass())
                                                            .build()),
                                            JdbcWriterConfig.builder().build(),
                                            RecordExtractor.identity());
                            jdbcRowWriter.close();
                        })
                .hasRootCauseInstanceOf(SQLException.class)
                .hasRootCauseMessage(expectedMsg);
    }

    @Test
    public void testInvalidQuery() {
        String expectedMsg = "unable to open JDBC writer";
        setMockContextObjectReusable(true);
        assertThatThrownBy(
                        () -> {
                            jdbcRowWriter =
                                    new JdbcRowWriter(
                                            mockContext,
                                            null,
                                            "iamnotsql",
                                            JdbcWriterState.empty(),
                                            new SimpleJdbcConnectionProvider(
                                                    new JdbcConnectionOptions
                                                                    .JdbcConnectionOptionsBuilder()
                                                            .withUrl(DERBY_EBOOKSHOP_DB.getUrl())
                                                            .withDriverName(
                                                                    DERBY_EBOOKSHOP_DB
                                                                            .getDriverClass())
                                                            .build()),
                                            JdbcWriterConfig.builder().build(),
                                            RecordExtractor.identity());
                            jdbcRowWriter.close();
                        })
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(expectedMsg);
    }

    @Test
    public void testIncompleteConfiguration() {
        String expectedMsg = "jdbc url is empty";
        try {
            jdbcRowWriter =
                    new JdbcRowWriter(
                            mockContext,
                            null,
                            String.format(INSERT_TEMPLATE, INPUT_TABLE),
                            JdbcWriterState.empty(),
                            new SimpleJdbcConnectionProvider(
                                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                            .withUrl(DERBY_EBOOKSHOP_DB.getUrl())
                                            .withDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                                            .build()),
                            JdbcWriterConfig.builder().build(),
                            RecordExtractor.identity());
            jdbcRowWriter.close();
        } catch (Exception e) {
            assertThat(findThrowable(e, NullPointerException.class)).isPresent();
            assertThat(findThrowableWithMessage(e, expectedMsg)).isPresent();
        }
    }

    @Test
    public void testIncompatibleTypes() {
        String expectedMsg = "Invalid character string format for type INTEGER.";
        try {
            setMockContextObjectReusable(true);
            jdbcRowWriter =
                    new JdbcRowWriter(
                            mockContext,
                            null,
                            String.format(INSERT_TEMPLATE, OUTPUT_TABLE),
                            JdbcWriterState.empty(),
                            new SimpleJdbcConnectionProvider(
                                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                            .withUrl(DERBY_EBOOKSHOP_DB.getUrl())
                                            .withDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                                            .build()),
                            JdbcWriterConfig.builder().build(),
                            RecordExtractor.identity());

            Row row = new Row(5);
            row.setField(0, 4);
            row.setField(1, "hello");
            row.setField(2, "world");
            row.setField(3, 0.99);
            row.setField(4, "imthewrongtype");

            jdbcRowWriter.write(row, Mockito.mock(SinkWriter.Context.class));
            jdbcRowWriter.close();
        } catch (Exception e) {
            assertThat(findThrowable(e, SQLDataException.class)).isPresent();
            assertThat(findThrowableWithMessage(e, expectedMsg)).isPresent();
        }
    }

    @Test
    public void testExceptionOnInvalidType() {
        String expectedMsg = "field index: 3, field value: 0.";
        try {
            setMockContextObjectReusable(true);
            jdbcRowWriter =
                    new JdbcRowWriter(
                            mockContext,
                            new int[] {
                                Types.INTEGER,
                                Types.VARCHAR,
                                Types.VARCHAR,
                                Types.DOUBLE,
                                Types.INTEGER
                            },
                            String.format(INSERT_TEMPLATE, OUTPUT_TABLE),
                            JdbcWriterState.empty(),
                            new SimpleJdbcConnectionProvider(
                                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                            .withUrl(DERBY_EBOOKSHOP_DB.getUrl())
                                            .withDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                                            .build()),
                            JdbcWriterConfig.builder().build(),
                            RecordExtractor.identity());

            JdbcTestFixture.TestEntry entry = TEST_DATA[0];
            Row row = new Row(5);
            row.setField(0, entry.id);
            row.setField(1, entry.title);
            row.setField(2, entry.author);
            row.setField(3, 0L); // use incompatible type (Long instead of Double)
            row.setField(4, entry.qty);
            jdbcRowWriter.write(row, Mockito.mock(SinkWriter.Context.class));
            jdbcRowWriter.close();
        } catch (Exception e) {
            assertThat(findThrowable(e, ClassCastException.class)).isPresent();
            assertThat(findThrowableWithMessage(e, expectedMsg)).isPresent();
        }
    }

    @Test
    public void testExceptionOnClose() {
        String expectedMsg = "Writing records to JDBC failed.";
        try {
            setMockContextObjectReusable(true);
            jdbcRowWriter =
                    new JdbcRowWriter(
                            mockContext,
                            new int[] {
                                Types.INTEGER,
                                Types.VARCHAR,
                                Types.VARCHAR,
                                Types.DOUBLE,
                                Types.INTEGER
                            },
                            String.format(INSERT_TEMPLATE, OUTPUT_TABLE),
                            JdbcWriterState.empty(),
                            new SimpleJdbcConnectionProvider(
                                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                            .withUrl(DERBY_EBOOKSHOP_DB.getUrl())
                                            .withDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                                            .build()),
                            JdbcWriterConfig.builder().build(),
                            RecordExtractor.identity());

            JdbcTestFixture.TestEntry entry = TEST_DATA[0];
            Row row = new Row(5);
            row.setField(0, entry.id);
            row.setField(1, entry.title);
            row.setField(2, entry.author);
            row.setField(3, entry.price);
            row.setField(4, entry.qty);
            SinkWriter.Context context = Mockito.mock(SinkWriter.Context.class);
            jdbcRowWriter.write(row, context);
            jdbcRowWriter.write(
                    row,
                    context); // writing the same record twice must yield a unique key violation.
            jdbcRowWriter.close();
        } catch (Exception e) {
            assertThat(findThrowable(e, RuntimeException.class)).isPresent();
            assertThat(findThrowableWithMessage(e, expectedMsg)).isPresent();
        }
    }

    @Test
    public void testJdbcRowWriter() throws Exception {
        setMockContextObjectReusable(true);
        jdbcRowWriter =
                new JdbcRowWriter(
                        mockContext,
                        null,
                        String.format(INSERT_TEMPLATE, OUTPUT_TABLE),
                        JdbcWriterState.empty(),
                        new SimpleJdbcConnectionProvider(
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl(DERBY_EBOOKSHOP_DB.getUrl())
                                        .withAutoCommit(true)
                                        .withDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                                        .build()),
                        JdbcWriterConfig.builder()
                                .setJdbcExecutionOptions(
                                        JdbcExecutionOptions.builder().withMaxRetries(0).build())
                                .build(),
                        RecordExtractor.identity());

        SinkWriter.Context context = Mockito.mock(SinkWriter.Context.class);
        for (JdbcTestFixture.TestEntry entry : TEST_DATA) {
            jdbcRowWriter.write(toRow(entry), context);
        }

        jdbcRowWriter.close();

        try (Connection dbConn = DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl());
                PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS);
                ResultSet resultSet = statement.executeQuery()) {
            int recordCount = 0;
            while (resultSet.next()) {
                assertThat(resultSet.getObject("id")).isEqualTo(TEST_DATA[recordCount].id);
                assertThat(resultSet.getObject("title")).isEqualTo(TEST_DATA[recordCount].title);
                assertThat(resultSet.getObject("author")).isEqualTo(TEST_DATA[recordCount].author);
                assertThat(resultSet.getObject("price")).isEqualTo(TEST_DATA[recordCount].price);
                assertThat(resultSet.getObject("qty")).isEqualTo(TEST_DATA[recordCount].qty);

                recordCount++;
            }
            assertThat(recordCount).isEqualTo(TEST_DATA.length);
        }
    }

    @Test
    public void testFlush() throws Exception {
        setMockContextObjectReusable(true);
        jdbcRowWriter =
                new JdbcRowWriter(
                        mockContext,
                        null,
                        String.format(INSERT_TEMPLATE, OUTPUT_TABLE_2),
                        JdbcWriterState.empty(),
                        new SimpleJdbcConnectionProvider(
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl(DERBY_EBOOKSHOP_DB.getUrl())
                                        .withAutoCommit(true)
                                        .withDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                                        .build()),
                        JdbcWriterConfig.builder()
                                .setJdbcExecutionOptions(
                                        JdbcExecutionOptions.builder().withBatchSize(3).build())
                                .build(),
                        RecordExtractor.identity());

        SinkWriter.Context context = Mockito.mock(SinkWriter.Context.class);

        try (Connection dbConn = DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl());
                PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS_2)) {
            for (int i = 0; i < 2; ++i) {
                jdbcRowWriter.write(toRow(TEST_DATA[i]), context);
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                assertThat(resultSet.next()).isFalse();
            }
            jdbcRowWriter.write(toRow(TEST_DATA[2]), context);
            try (ResultSet resultSet = statement.executeQuery()) {
                int recordCount = 0;
                while (resultSet.next()) {
                    assertThat(resultSet.getObject("id")).isEqualTo(TEST_DATA[recordCount].id);
                    assertThat(resultSet.getObject("title"))
                            .isEqualTo(TEST_DATA[recordCount].title);
                    assertThat(resultSet.getObject("author"))
                            .isEqualTo(TEST_DATA[recordCount].author);
                    assertThat(resultSet.getObject("price"))
                            .isEqualTo(TEST_DATA[recordCount].price);
                    assertThat(resultSet.getObject("qty")).isEqualTo(TEST_DATA[recordCount].qty);
                    recordCount++;
                }
                assertThat(recordCount).isEqualTo(3);
            }
        } finally {
            jdbcRowWriter.close();
        }
    }

    @Test
    public void testInvalidConnectionInJdbcRowWriter() throws Exception {
        setMockContextObjectReusable(true);
        jdbcRowWriter =
                new JdbcRowWriter(
                        mockContext,
                        null,
                        String.format(INSERT_TEMPLATE, OUTPUT_TABLE_3),
                        JdbcWriterState.empty(),
                        new SimpleJdbcConnectionProvider(
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl(DERBY_EBOOKSHOP_DB.getUrl())
                                        .withAutoCommit(true)
                                        .withDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                                        .build()),
                        JdbcWriterConfig.builder()
                                .setJdbcExecutionOptions(JdbcExecutionOptions.builder().build())
                                .build(),
                        RecordExtractor.identity());

        SinkWriter.Context context = Mockito.mock(SinkWriter.Context.class);

        // write records
        for (int i = 0; i < 3; i++) {
            jdbcRowWriter.write(toRow(TEST_DATA[i]), context);
        }

        // close connection
        jdbcRowWriter.getConnection().close();

        for (int i = 3; i < TEST_DATA.length; i++) {
            jdbcRowWriter.write(toRow(TEST_DATA[i]), context);
        }

        jdbcRowWriter.close();

        try (Connection dbConn = DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl());
                PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS_3);
                ResultSet resultSet = statement.executeQuery()) {
            int recordCount = 0;
            while (resultSet.next()) {
                assertThat(resultSet.getObject("id")).isEqualTo(TEST_DATA[recordCount].id);
                assertThat(resultSet.getObject("title")).isEqualTo(TEST_DATA[recordCount].title);
                assertThat(resultSet.getObject("author")).isEqualTo(TEST_DATA[recordCount].author);
                assertThat(resultSet.getObject("price")).isEqualTo(TEST_DATA[recordCount].price);
                assertThat(resultSet.getObject("qty")).isEqualTo(TEST_DATA[recordCount].qty);

                recordCount++;
            }
            assertThat(recordCount).isEqualTo(TEST_DATA.length);
        }
    }
}
