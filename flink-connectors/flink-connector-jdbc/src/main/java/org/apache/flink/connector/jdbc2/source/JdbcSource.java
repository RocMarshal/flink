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

package org.apache.flink.connector.jdbc2.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.jdbc2.source.enumerator.JdbcSourceEnumeratorState;
import org.apache.flink.connector.jdbc2.source.enumerator.JdbcSourceEnumStateSerializer;
import org.apache.flink.connector.jdbc2.source.reader.JdbcSourceReader;
import org.apache.flink.connector.jdbc2.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc2.source.split.JdbcSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

public class JdbcSource implements Source<Row, JdbcSourceSplit, JdbcSourceEnumeratorState>,
        ResultTypeQueryable<Row> {

    private Boundedness boundedness;
    private TypeInformation<Row> typeInformation;

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<Row, JdbcSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new JdbcSourceReader();
    }

    @Override
    public SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> enumContext,
            JdbcSourceEnumeratorState checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<JdbcSourceSplit> getSplitSerializer() {
        return new JdbcSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<JdbcSourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new JdbcSourceEnumStateSerializer();
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return typeInformation;
    }
}
