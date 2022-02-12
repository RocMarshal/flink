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

package org.apache.flink.connector.jdbc2.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.jdbc2.source.split.JdbcSourceSplit;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class JdbcSourceEnumerator implements SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceEnumerator.class);

    private final SplitEnumeratorContext<JdbcSourceSplit> context;
    private final Boundedness boundedness;

    public JdbcSourceEnumerator(SplitEnumeratorContext<JdbcSourceSplit> context, Boundedness boundedness) {
        this.context = Preconditions.checkNotNull(context);
        this.boundedness = boundedness;
    }

    @Override
    public void start() {
        if (boundedness == Boundedness.CONTINUOUS_UNBOUNDED) {
            // TODO
        }
    }

    @Override
    public void close() throws IOException {
        if (boundedness == Boundedness.CONTINUOUS_UNBOUNDED) {
            // TODO
        }
    }

    @Override
    public void addReader(int subtaskId) { }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (boundedness == Boundedness.CONTINUOUS_UNBOUNDED) {
            // TODO
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (boundedness == Boundedness.CONTINUOUS_UNBOUNDED) {
            // TODO
        }
    }

    @Override
    public void handleSplitRequest(int subtask, @Nullable String hostname) {
        if (!context.registeredReaders().containsKey(subtask)) {
            return;
        }
        if (LOG.isInfoEnabled()) {
            final String hostInfo =
                    hostname == null ? "(no host locality info)" : "(on host '" + hostname + "')";
            LOG.info("Subtask {} {} is requesting a Jdbc source split", subtask, hostInfo);
        }
        final Optional<JdbcSourceSplit> nextSplit = getNextSplit();
        if (nextSplit.isPresent()) {
            final JdbcSourceSplit split = nextSplit.get();
            context.assignSplit(split, subtask);
            LOG.info("Assigned split to subtask {} : {}", subtask, split);
        } else {
            context.signalNoMoreSplits(subtask);
            LOG.info("No more splits available for subtask {}", subtask);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.error("Received unrecognized event: {}", sourceEvent);
    }

    // TODO
    @Override
    public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {

    }

    // TODO
    @Override
    public JdbcSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    // TODO
    Optional<JdbcSourceSplit> getNextSplit() {
        return Optional.empty();
    }

}
