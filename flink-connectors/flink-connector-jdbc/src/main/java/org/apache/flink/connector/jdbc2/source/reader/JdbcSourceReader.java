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

package org.apache.flink.connector.jdbc2.source.reader;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.connector.jdbc2.source.split.JdbcSourceSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class JdbcSourceReader implements SourceReader<Row, JdbcSourceSplit> {


    @Override
    public void start() {

    }

    @Override
    public InputStatus pollNext(ReaderOutput<Row> output) throws Exception {
        return null;
    }

    @Override
    public List<JdbcSourceSplit> snapshotState(long checkpointId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return null;
    }

    @Override
    public void addSplits(List<JdbcSourceSplit> splits) {

    }

    @Override
    public void notifyNoMoreSplits() {

    }

    @Override
    public void close() throws Exception {

    }
}
