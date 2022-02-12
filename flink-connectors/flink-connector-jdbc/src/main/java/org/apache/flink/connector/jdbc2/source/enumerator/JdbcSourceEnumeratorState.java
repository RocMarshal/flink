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

import org.apache.flink.connector.jdbc2.source.split.JdbcSourceSplit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class JdbcSourceEnumeratorState implements Serializable {

    private List<JdbcSourceSplit> completedSplits;
    private List<JdbcSourceSplit> pendingSplits;
    private List<JdbcSourceSplit> remainingSplits;

    public JdbcSourceEnumeratorState(
            List<JdbcSourceSplit> completedSplits,
            List<JdbcSourceSplit> pendingSplits,
            List<JdbcSourceSplit> remainingSplits) {
        this.completedSplits = completedSplits;
        this.pendingSplits = pendingSplits;
        this.remainingSplits = remainingSplits;
    }

    public JdbcSourceEnumeratorState() {
        this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
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
