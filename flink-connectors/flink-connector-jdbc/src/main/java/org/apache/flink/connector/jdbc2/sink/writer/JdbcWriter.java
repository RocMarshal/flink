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

package org.apache.flink.connector.jdbc2.sink.writer;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.jdbc2.sink.commit.JdbcCommittable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class JdbcWriter<IN> implements StatefulSink.StatefulSinkWriter<IN, JdbcWriterState>,
        TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, JdbcCommittable>{
    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {

    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {

    }

    @Override
    public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
        StatefulSink.StatefulSinkWriter.super.writeWatermark(watermark);
    }

    @Override
    public List<JdbcWriterState> snapshotState(long checkpointId) throws IOException {
        return null;
    }

    @Override
    public Collection<JdbcCommittable> prepareCommit() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
