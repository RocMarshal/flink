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

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.List;

import static org.apache.flink.sql.parser.utils.SqlDdlUnParseUtil.unParseTableDdlBody;

/** Class for ALTER TABLE ADD multi columns clause. */
public class SqlAlterTableAddColumns extends SqlAlterTable {

    protected final SqlNodeList columnList;
    protected final SqlWatermark watermark;
    protected final List<SqlTableConstraint> tableConstraints;

    public SqlAlterTableAddColumns(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            SqlWatermark sqlWatermark,
            List<SqlTableConstraint> tableConstraints) {
        super(pos, tableName, null);
        this.columnList = columnList;
        this.watermark = sqlWatermark;
        this.tableConstraints = tableConstraints;
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    public SqlWatermark getWatermark() {
        return watermark;
    }

    public List<SqlTableConstraint> getTableConstraints() {
        return tableConstraints;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                getTableName(),
                this.columnList,
                this.watermark,
                new SqlNodeList(tableConstraints, SqlParserPos.ZERO));
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("ADD");
        unParseTableDdlBody(writer, leftPrec, rightPrec, columnList, tableConstraints, watermark);
    }
}
