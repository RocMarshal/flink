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

package org.apache.flink.connector.jdbc.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * State of the reader, essentially a mutable version of the {@link JdbcSourceSplit}. Has a
 * modifiable offset.
 *
 * <p>The {@link JdbcSourceSplit} assigned to the reader or stored in the checkpoint points to the
 * position from where to start reading (after recovery), so the current offset need to always point
 * to the record after the last emitted record.
 */
@Internal
public class JdbcSourceSplitState<SplitT extends JdbcSourceSplit> implements Serializable {

    private final SplitT split;

    private int offset;

    public JdbcSourceSplitState(SplitT split) {
        this.split = checkNotNull(split);

        final int readerPosition = split.getReaderPosition();
        this.offset = readerPosition;
    }

    public void setPosition(int offset) {
        this.offset = offset;
    }

    /** Use the current row count as the starting row count to create a new FileSourceSplit. */
    @SuppressWarnings("unchecked")
    public SplitT toJdbcSourceSplit() {

        final JdbcSourceSplit updatedSplit = split.updateWithCheckpointedPosition(offset);

        // some sanity checks to avoid surprises and not accidentally lose split information
        if (updatedSplit == null) {
            throw new FlinkRuntimeException(
                    "Split returned 'null' in updateWithCheckpointedPosition(): " + split);
        }
        if (updatedSplit.getClass() != split.getClass()) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Split returned different type in updateWithCheckpointedPosition(). "
                                    + "Split type is %s, returned type is %s",
                            split.getClass().getName(), updatedSplit.getClass().getName()));
        }

        return (SplitT) updatedSplit;
    }
}
