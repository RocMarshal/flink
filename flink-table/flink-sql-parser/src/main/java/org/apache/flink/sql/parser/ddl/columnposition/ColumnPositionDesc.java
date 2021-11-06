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

package org.apache.flink.sql.parser.ddl.columnposition;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.sql.SqlIdentifier;

/** Util class for description of column order definition. */
@Internal
public class ColumnPositionDesc {

    public static final ColumnPositionDesc DEFAULT_POSIT =
            new ColumnPositionDesc(null, Preposition.DEFAULT);
    public static final ColumnPositionDesc FIRST_POSIT =
            new ColumnPositionDesc(null, Preposition.FIRST);

    private final SqlIdentifier referencedColumn;
    private final Preposition preposition;

    public static ColumnPositionDesc of(SqlIdentifier referencedColumn, Preposition preposition) {
        Preconditions.checkArgument(
                referencedColumn != null,
                "Invalid column location description for referenced column.");
        Preconditions.checkArgument(
                Preposition.AFTER == preposition,
                "Invalid column location description for preposition.");
        return new ColumnPositionDesc(referencedColumn, preposition);
    }

    private ColumnPositionDesc(SqlIdentifier referencedColumn, Preposition preposition) {
        this.referencedColumn = referencedColumn;
        this.preposition = preposition;
    }

    public SqlIdentifier getReferencedColumn() {
        return referencedColumn;
    }

    public Preposition getPreposition() {
        return preposition;
    }

    /** Enum of preposition of the referenced column . */
    public enum Preposition {
        FIRST,
        AFTER,
        DEFAULT
    }
}
