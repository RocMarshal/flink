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

package org.apache.flink.connector.jdbc.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.DbMetadata;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcTestBaseJUnit5;
import org.apache.flink.connector.jdbc.JdbcTestFixture;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectLoader;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.sink.writer.JdbcGenericWriter;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterConfig;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterState;
import org.apache.flink.connector.jdbc.sink.writer.RecordExtractor;
import org.apache.flink.connector.jdbc.sink.writer.StatementExecutorFactory;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.jdbc.JdbcDataTestBase.toRow;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.sink.writer.JdbcGenericWriter.createSimpleRowExecutor;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the Append only mode. */
public class JdbcAppendOnlyWriterTestJUnit5 extends JdbcTestBaseJUnit5 {

    SinkWriter.Context mockSinkWriterContext = Mockito.mock(SinkWriter.Context.class);

    Sink.InitContext mockContext =
            new Sink.InitContext() {
                private ExecutionConfig executionConfig = new ExecutionConfig();

                private JobID jobID = JobID.generate();

                @Override
                public UserCodeClassLoader getUserCodeClassLoader() {
                    return new UserCodeClassLoader() {
                        @Override
                        public ClassLoader asClassLoader() {
                            return Thread.currentThread().getContextClassLoader();
                        }

                        @Override
                        public void registerReleaseHookIfAbsent(
                                String releaseHookName, Runnable releaseHook) {}
                    };
                }

                @Override
                public JobID getJobId() {
                    return jobID;
                }

                @Override
                public ExecutionConfig getExecutionConfig() {

                    return executionConfig;
                }

                @Override
                public MailboxExecutor getMailboxExecutor() {
                    return null;
                }

                @Override
                public ProcessingTimeService getProcessingTimeService() {
                    return null;
                }

                @Override
                public int getSubtaskId() {
                    return 0;
                }

                @Override
                public int getNumberOfParallelSubtasks() {
                    return 1;
                }

                @Override
                public SinkWriterMetricGroup metricGroup() {
                    return null;
                }

                @Override
                public OptionalLong getRestoredCheckpointId() {
                    return OptionalLong.empty();
                }

                @Override
                public SerializationSchema.InitializationContext
                        asSerializationSchemaInitializationContext() {
                    return null;
                }
            };

    private JdbcGenericWriter jdbcGenericWriter;
    private String[] fieldNames;

    @BeforeEach
    public void setup() {
        fieldNames = new String[] {"id", "title", "author", "price", "qty"};
    }

    public void setMockContextObjectReusable(boolean reused) {
        if (reused) {
            mockContext.getExecutionConfig().enableObjectReuse();
            return;
        }
        mockContext.getExecutionConfig().disableObjectReuse();
    }

    @Test
    public void testMaxRetry() throws Exception {
        assertThatThrownBy(
                        () -> {
                            JdbcConnectorOptions jdbcOptions =
                                    JdbcConnectorOptions.builder()
                                            .setDBUrl(getDbMetadata().getUrl())
                                            .setDialect(
                                                    JdbcDialectLoader.load(
                                                            getDbMetadata().getUrl(),
                                                            getClass().getClassLoader()))
                                            .setTableName(OUTPUT_TABLE)
                                            .build();
                            JdbcDmlOptions dmlOptions =
                                    JdbcDmlOptions.builder()
                                            .withTableName(jdbcOptions.getTableName())
                                            .withDialect(jdbcOptions.getDialect())
                                            .withFieldNames(fieldNames)
                                            .build();

                            setMockContextObjectReusable(true);

                            JdbcWriterConfig jdbcWriterConfig =
                                    JdbcWriterConfig.builder()
                                            .setJdbcConnectionOptions(jdbcOptions)
                                            .setJdbcExecutionOptions(
                                                    JdbcExecutionOptions.builder().build())
                                            .build();
                            final String sql =
                                    dmlOptions
                                            .getDialect()
                                            .getInsertIntoStatement(
                                                    dmlOptions.getTableName(),
                                                    dmlOptions.getFieldNames());

                            jdbcGenericWriter =
                                    new JdbcGenericWriter<
                                            Tuple2<Boolean, Row>,
                                            Row,
                                            JdbcBatchStatementExecutor<Row>>(
                                            mockContext,
                                            JdbcWriterState.empty(),
                                            new SimpleJdbcConnectionProvider(
                                                    jdbcWriterConfig.getJdbcConnectionOptions()),
                                            jdbcWriterConfig,
                                            new StatementExecutorFactory<
                                                    JdbcBatchStatementExecutor<Row>>() {
                                                @Override
                                                public JdbcBatchStatementExecutor<Row> apply(
                                                        Sink.InitContext initContext) {
                                                    return createSimpleRowExecutor(
                                                            sql,
                                                            dmlOptions.getFieldTypes(),
                                                            initContext
                                                                    .getExecutionConfig()
                                                                    .isObjectReuseEnabled());
                                                }
                                            },
                                            new RecordExtractor<Tuple2<Boolean, Row>, Row>() {
                                                @Override
                                                public Row apply(
                                                        Tuple2<Boolean, Row> booleanRowTuple2) {
                                                    return booleanRowTuple2.f1;
                                                }
                                            }) {};

                            // alter table schema to trigger retry logic after failure.
                            alterTable();
                            for (JdbcTestFixture.TestEntry entry : TEST_DATA) {
                                jdbcGenericWriter.write(
                                        Tuple2.of(true, toRow(entry)), mockSinkWriterContext);
                            }

                            // after retry default times, throws a BatchUpdateException.
                            jdbcGenericWriter.flush(true);
                        })
                // TODO IOException.class
                .isInstanceOf(FlinkRuntimeException.class);
    }

    private void alterTable() throws Exception {
        Class.forName(getDbMetadata().getDriverClass());
        try (Connection conn = DriverManager.getConnection(getDbMetadata().getUrl());
                Statement stat = conn.createStatement()) {
            stat.execute("ALTER  TABLE " + OUTPUT_TABLE + " DROP COLUMN " + fieldNames[1]);
        }
    }

    @AfterEach
    public void clear() throws Exception {
        if (jdbcGenericWriter != null) {
            try {
                jdbcGenericWriter.close();
            } catch (RuntimeException e) {
                // ignore exception when close.
            }
        }
        jdbcGenericWriter = null;
        Class.forName(getDbMetadata().getDriverClass());
        try (Connection conn = DriverManager.getConnection(getDbMetadata().getUrl());
                Statement stat = conn.createStatement()) {
            stat.execute("DELETE FROM " + OUTPUT_TABLE);
        }
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return DERBY_EBOOKSHOP_DB;
    }
}
