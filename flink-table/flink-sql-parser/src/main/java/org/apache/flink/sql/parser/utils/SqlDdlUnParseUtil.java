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

package org.apache.flink.sql.parser.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;

import java.util.List;
import java.util.Objects;

/** Util class for un-parsing ddl operation. */
@Internal
public class SqlDdlUnParseUtil {

    private SqlDdlUnParseUtil() {}

    public static void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }

    public static void unParseAddReplaceColumns(
            SqlWriter writer, int leftPrec, int rightPrec, List<SqlNode> newColumns) {
        writer.keyword("COLUMNS");
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        for (SqlNode column : newColumns) {
            printIndent(writer);
            column.unparse(writer, leftPrec, rightPrec);
        }
        writer.newlineAndIndent();
        writer.endList(frame);
    }

    public static void unParseTableDdlBody(
            SqlWriter writer,
            int leftPrec,
            int rightPrec,
            SqlNodeList columnList,
            List<SqlTableConstraint> tableConstraints,
            SqlWatermark watermark) {
        if ((Objects.nonNull(columnList) && columnList.size() > 0)
                || (Objects.nonNull(tableConstraints) && tableConstraints.size() > 0)
                || watermark != null) {
            SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
            for (SqlNode column : columnList) {
                printIndent(writer);
                column.unparse(writer, leftPrec, rightPrec);
            }
            if (tableConstraints.size() > 0) {
                for (SqlTableConstraint constraint : tableConstraints) {
                    printIndent(writer);
                    constraint.unparse(writer, leftPrec, rightPrec);
                }
            }
            if (watermark != null) {
                printIndent(writer);
                watermark.unparse(writer, leftPrec, rightPrec);
            }

            writer.newlineAndIndent();
            writer.endList(frame);
        }
    }

    public static void unParseTableDdlComment(
            SqlWriter writer, int leftPrec, int rightPrec, SqlCharStringLiteral comment) {
        if (Objects.nonNull(comment)) {
            writer.newlineAndIndent();
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
        }
    }

    public static void unParseDatabaseDdlComment(
            SqlWriter writer, int leftPrec, int rightPrec, SqlCharStringLiteral comment) {
        if (Objects.nonNull(comment)) {
            writer.newlineAndIndent();
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
        }
    }

    public static void unParseTableDdlPartitionKeys(
            SqlWriter writer, int leftPrec, int rightPrec, SqlNodeList partitionKeyList) {
        if (Objects.nonNull(partitionKeyList) && partitionKeyList.size() > 0) {
            writer.newlineAndIndent();
            writer.keyword("PARTITIONED BY");
            SqlWriter.Frame partitionedByFrame = writer.startList("(", ")");
            partitionKeyList.unparse(writer, leftPrec, rightPrec);
            writer.endList(partitionedByFrame);
            writer.newlineAndIndent();
        }
    }

    public static void unParseTableDdlProps(
            SqlWriter writer, int leftPrec, int rightPrec, SqlNodeList propertyList) {
        if (Objects.nonNull(propertyList) && propertyList.size() > 0) {
            writer.keyword("WITH");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : propertyList) {
                printIndent(writer);
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }
    }

    public static void unParseCatalogDdlProps(
            SqlWriter writer, int leftPrec, int rightPrec, SqlNodeList propertyList) {
        unParseTableDdlProps(writer, leftPrec, rightPrec, propertyList);
    }

    public static void unParseDatabaseDdlProps(
            SqlWriter writer, int leftPrec, int rightPrec, SqlNodeList propertyList) {
        unParseTableDdlProps(writer, leftPrec, rightPrec, propertyList);
    }
}
