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
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * A {@link SourceSplit} that represents a JDBC sqlTemplate with optional parameters.
 *
 * <p>The split has an offset, which defines the offset of the queries {@link java.sql.ResultSet}
 * for the split. The offset is the checkpointed position from a reader previously reading this
 * split. This position is typically zero when the split is assigned from the enumerator to the
 * readers, and is positive when the readers checkpoint their state in a Jdbc source split.
 */
@Internal
public class JdbcSourceSplit implements SourceSplit, Serializable {

    @Nullable JdbcConnectionOptions jdbcConnectionOptions;

    private final String id;

    private final String sqlTemplate;

    private final @Nullable Serializable[] parameters;

    private final int offset;

    public JdbcSourceSplit(
            String id, String sqlTemplate, @Nullable Serializable[] parameters, int offset) {
        this.id = id;
        this.sqlTemplate = sqlTemplate;
        this.parameters = parameters;
        this.offset = offset;
    }

    public int getOffset() {
        return offset;
    }

    public JdbcSourceSplit updateWithCheckpointedPosition(int offset) {
        return new JdbcSourceSplit(id, sqlTemplate, parameters, offset);
    }

    public int getReaderPosition() {
        return offset;
    }

    public String getSqlTemplate() {
        return sqlTemplate;
    }

    @Nullable
    public Object[] getParameters() {
        return parameters;
    }

    @Override
    public String splitId() {
        return id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, sqlTemplate, Arrays.hashCode(parameters), offset);
    }

    @Override
    public boolean equals(Object obj) {

        if (Objects.isNull(obj) || !(obj instanceof JdbcSourceSplit)) {
            return false;
        }
        JdbcSourceSplit other = (JdbcSourceSplit) obj;
        return Objects.equals(id, other.id)
                && Objects.equals(sqlTemplate, other.sqlTemplate)
                && Arrays.equals(parameters, other.parameters)
                && Objects.equals(offset, other.offset);
    }
}
