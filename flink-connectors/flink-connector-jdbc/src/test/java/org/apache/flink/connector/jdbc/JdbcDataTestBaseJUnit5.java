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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;
import org.apache.flink.util.UserCodeClassLoader;

import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.OptionalLong;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;

/**
 * Base class for JDBC test using data from {@link JdbcTestFixture}. It uses {@link DerbyDbMetadata}
 * and inserts data before each test.
 */
public abstract class JdbcDataTestBaseJUnit5 extends JdbcTestBaseJUnit5 {

    protected SinkWriter.Context mockSinkWriterContext = Mockito.mock(SinkWriter.Context.class);

    protected Sink.InitContext mockContext =
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

    public void setMockContextObjectReusable(boolean reused) {
        if (reused) {
            mockContext.getExecutionConfig().enableObjectReuse();
            return;
        }
        mockContext.getExecutionConfig().disableObjectReuse();
    }

    @BeforeEach
    public void initData() throws SQLException {
        JdbcTestFixture.initData(getDbMetadata());
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return DERBY_EBOOKSHOP_DB;
    }

    public static Row toRow(JdbcTestFixture.TestEntry entry) {
        Row row = new Row(5);
        row.setField(0, entry.id);
        row.setField(1, entry.title);
        row.setField(2, entry.author);
        row.setField(3, entry.price);
        row.setField(4, entry.qty);
        return row;
    }

    // utils function to build a RowData, note: only support primitive type and String from now
    public static RowData buildGenericData(Object... args) {
        GenericRowData row = new GenericRowData(args.length);
        for (int i = 0; i < args.length; i++) {
            if (args[i] instanceof String) {
                row.setField(i, StringData.fromString((String) args[i]));
            } else {
                row.setField(i, args[i]);
            }
        }
        return row;
    }
}
