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

package org.apache.flink.connector.jdbc2.sink;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.jdbc2.sink.commit.JdbcCommittable;
import org.apache.flink.connector.jdbc2.sink.writer.JdbcWriter;
import org.apache.flink.connector.jdbc2.sink.writer.JdbcWriterState;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;

public class JdbcSink<IN> implements StatefulSink<IN, JdbcWriterState>,
        TwoPhaseCommittingSink<IN, JdbcCommittable> {

    @Override
    public Committer<JdbcCommittable> createCommitter() throws IOException {
        return null;
    }

    public static <IN> JdbcSinkBuilder<IN> builder() {
        return new JdbcSinkBuilder<>();
    }

    @Override
    public SimpleVersionedSerializer<JdbcCommittable> getCommittableSerializer() {
        return null;
    }

    @Override
    public JdbcWriter<IN> createWriter(InitContext context) throws IOException {
        return null;
    }

    @Override
    public StatefulSinkWriter<IN, JdbcWriterState> restoreWriter(
            InitContext context,
            Collection<JdbcWriterState> recoveredState) throws IOException {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<JdbcWriterState> getWriterStateSerializer() {
        return null;
    }
}
