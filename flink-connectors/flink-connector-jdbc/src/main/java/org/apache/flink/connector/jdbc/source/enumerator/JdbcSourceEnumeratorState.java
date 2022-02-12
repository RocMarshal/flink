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

package org.apache.flink.connector.jdbc.source.enumerator;

import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** JdbcSourceEnumeratorState. */
public class JdbcSourceEnumeratorState implements Serializable {

    private @Nonnull List<JdbcSourceSplit> completedSplits;
    private @Nonnull List<JdbcSourceSplit> pendingSplits;
    private @Nonnull List<JdbcSourceSplit> remainingSplits;

    private final @Nullable Serializable optionalUserDefinedSplitEnumeratorState;

    public JdbcSourceEnumeratorState(
            List<JdbcSourceSplit> completedSplits,
            List<JdbcSourceSplit> pendingSplits,
            List<JdbcSourceSplit> remainingSplits,
            Serializable optionalUserDefinedSplitEnumeratorState) {
        this.completedSplits = Preconditions.checkNotNull(completedSplits);
        this.pendingSplits = Preconditions.checkNotNull(pendingSplits);
        this.remainingSplits = Preconditions.checkNotNull(remainingSplits);
        this.optionalUserDefinedSplitEnumeratorState = optionalUserDefinedSplitEnumeratorState;
    }

    public Serializable getOptionalUserDefinedSplitEnumeratorState() {
        return optionalUserDefinedSplitEnumeratorState;
    }

    public JdbcSourceEnumeratorState() {
        this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), null);
    }

    public List<JdbcSourceSplit> getCompletedSplits() {
        return completedSplits;
    }

    public void setCompletedSplits(List<JdbcSourceSplit> completedSplits) {
        this.completedSplits = completedSplits;
    }

    public List<JdbcSourceSplit> getPendingSplits() {
        return pendingSplits;
    }

    public void setPendingSplits(List<JdbcSourceSplit> pendingSplits) {
        this.pendingSplits = pendingSplits;
    }

    public List<JdbcSourceSplit> getRemainingSplits() {
        return remainingSplits;
    }

    public void setRemainingSplits(List<JdbcSourceSplit> remainingSplits) {
        this.remainingSplits = remainingSplits;
    }
}
