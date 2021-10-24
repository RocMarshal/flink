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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlAddPartitions;
import org.apache.flink.sql.parser.ddl.SqlAddReplaceColumns;
import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.SqlAlterTableAddColumn;
import org.apache.flink.sql.parser.ddl.SqlAlterTableAddColumns;
import org.apache.flink.sql.parser.ddl.SqlAlterTableAddConstraint;
import org.apache.flink.sql.parser.ddl.SqlAlterTableAddWatermark;
import org.apache.flink.sql.parser.ddl.SqlAlterTableCompact;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropConstraint;
import org.apache.flink.sql.parser.ddl.SqlAlterTableOptions;
import org.apache.flink.sql.parser.ddl.SqlAlterTableRename;
import org.apache.flink.sql.parser.ddl.SqlAlterTableReset;
import org.apache.flink.sql.parser.ddl.SqlChangeColumn;
import org.apache.flink.sql.parser.ddl.SqlDropPartitions;
import org.apache.flink.sql.parser.ddl.SqlTableLike;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ManagedTableListener;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AddPartitionsOperation;
import org.apache.flink.table.operations.ddl.AlterPartitionPropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterTableAddColumnOperation;
import org.apache.flink.table.operations.ddl.AlterTableAddColumnsOperation;
import org.apache.flink.table.operations.ddl.AlterTableAddConstraintOperation;
import org.apache.flink.table.operations.ddl.AlterTableAddWatermarkOperation;
import org.apache.flink.table.operations.ddl.AlterTableCompactOperation;
import org.apache.flink.table.operations.ddl.AlterTableDropConstraintOperation;
import org.apache.flink.table.operations.ddl.AlterTableOperation;
import org.apache.flink.table.operations.ddl.AlterTableOptionsOperation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;
import org.apache.flink.table.operations.ddl.DropPartitionsOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.utils.OperationConverterUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Helper class for converting {@link SqlAlterTable} to {@link AlterTableOperation}. */
public class SqlAlterTableConverter {

    private final FlinkPlannerImpl flinkPlanner;
    private final CatalogManager catalogManager;
    private final Consumer<SqlTableConstraint> validateTableConstraint;
    private final MergeTableLikeUtil mergeTableLikeUtil;
    private final Function<SqlNode, String> escapeExpression;

    SqlAlterTableConverter(
            FlinkPlannerImpl flinkPlanner,
            CatalogManager catalogManager,
            Function<SqlNode, String> escapeExpression,
            Consumer<SqlTableConstraint> validateTableConstraint) {
        this.flinkPlanner = flinkPlanner;
        this.catalogManager = catalogManager;
        this.validateTableConstraint = validateTableConstraint;
        this.escapeExpression = escapeExpression;
        this.mergeTableLikeUtil =
                new MergeTableLikeUtil(flinkPlanner.getOrCreateSqlValidator(), escapeExpression);
    }

    /** convert ALTER TABLE statement. */
    public Operation convertAlterTable(SqlAlterTable sqlAlterTable) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterTable.fullTableName());
        ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        Optional<ContextResolvedTable> optionalCatalogTable =
                catalogManager.getTable(tableIdentifier);
        if (!optionalCatalogTable.isPresent() || optionalCatalogTable.get().isTemporary()) {
            throw new ValidationException(
                    String.format(
                            "Table %s doesn't exist or is a temporary table.", tableIdentifier));
        }
        CatalogBaseTable baseTable = optionalCatalogTable.get().getTable();
        if (baseTable instanceof CatalogView) {
            throw new ValidationException("ALTER TABLE for a view is not allowed");
        }
        if (sqlAlterTable instanceof SqlAlterTableRename) {
            UnresolvedIdentifier newUnresolvedIdentifier =
                    UnresolvedIdentifier.of(
                            ((SqlAlterTableRename) sqlAlterTable).fullNewTableName());
            ObjectIdentifier newTableIdentifier =
                    catalogManager.qualifyIdentifier(newUnresolvedIdentifier);
            return new AlterTableRenameOperation(tableIdentifier, newTableIdentifier);
        } else if (sqlAlterTable instanceof SqlAlterTableOptions) {
            return convertAlterTableOptions(
                    tableIdentifier,
                    (CatalogTable) baseTable,
                    (SqlAlterTableOptions) sqlAlterTable);
        } else if (sqlAlterTable instanceof SqlAlterTableReset) {
            return convertAlterTableReset(
                    tableIdentifier, (CatalogTable) baseTable, (SqlAlterTableReset) sqlAlterTable);
        } else if (sqlAlterTable instanceof SqlAlterTableAddConstraint) {
            SqlTableConstraint constraint =
                    ((SqlAlterTableAddConstraint) sqlAlterTable).getConstraint();
            validateTableConstraint.accept(constraint);
            TableSchema oriSchema =
                    TableSchema.fromResolvedSchema(
                            baseTable
                                    .getUnresolvedSchema()
                                    .resolve(catalogManager.getSchemaResolver()));
            // Sanity check for constraint.
            TableSchema.Builder builder = TableSchemaUtils.builderWithGivenSchema(oriSchema);
            if (constraint.getConstraintName().isPresent()) {
                builder.primaryKey(
                        constraint.getConstraintName().get(), constraint.getColumnNames());
            } else {
                builder.primaryKey(constraint.getColumnNames());
            }
            builder.build();
            return new AlterTableAddConstraintOperation(
                    tableIdentifier,
                    constraint.getConstraintName().orElse(null),
                    constraint.getColumnNames());
        } else if (sqlAlterTable instanceof SqlAlterTableDropConstraint) {
            SqlAlterTableDropConstraint dropConstraint =
                    ((SqlAlterTableDropConstraint) sqlAlterTable);
            String constraintName = dropConstraint.getConstraintName().getSimple();
            TableSchema oriSchema =
                    TableSchema.fromResolvedSchema(
                            baseTable
                                    .getUnresolvedSchema()
                                    .resolve(catalogManager.getSchemaResolver()));
            if (!oriSchema
                    .getPrimaryKey()
                    .filter(pk -> pk.getName().equals(constraintName))
                    .isPresent()) {
                throw new ValidationException(
                        String.format("CONSTRAINT [%s] does not exist", constraintName));
            }
            return new AlterTableDropConstraintOperation(tableIdentifier, constraintName);
        } else if (sqlAlterTable instanceof SqlAddReplaceColumns) {
            return OperationConverterUtils.convertAddReplaceColumns(
                    tableIdentifier,
                    (SqlAddReplaceColumns) sqlAlterTable,
                    (CatalogTable) baseTable,
                    flinkPlanner.getOrCreateSqlValidator());
        } else if (sqlAlterTable instanceof SqlChangeColumn) {
            return OperationConverterUtils.convertChangeColumn(
                    tableIdentifier,
                    (SqlChangeColumn) sqlAlterTable,
                    (CatalogTable) baseTable,
                    flinkPlanner.getOrCreateSqlValidator());
        } else if (sqlAlterTable instanceof SqlAddPartitions) {
            List<CatalogPartitionSpec> specs = new ArrayList<>();
            List<CatalogPartition> partitions = new ArrayList<>();
            SqlAddPartitions addPartitions = (SqlAddPartitions) sqlAlterTable;
            for (int i = 0; i < addPartitions.getPartSpecs().size(); i++) {
                specs.add(new CatalogPartitionSpec(addPartitions.getPartitionKVs(i)));
                Map<String, String> props =
                        OperationConverterUtils.extractProperties(
                                addPartitions.getPartProps().get(i));
                partitions.add(new CatalogPartitionImpl(props, null));
            }
            return new AddPartitionsOperation(
                    tableIdentifier, addPartitions.ifNotExists(), specs, partitions);
        } else if (sqlAlterTable instanceof SqlDropPartitions) {
            SqlDropPartitions dropPartitions = (SqlDropPartitions) sqlAlterTable;
            List<CatalogPartitionSpec> specs = new ArrayList<>();
            for (int i = 0; i < dropPartitions.getPartSpecs().size(); i++) {
                specs.add(new CatalogPartitionSpec(dropPartitions.getPartitionKVs(i)));
            }
            return new DropPartitionsOperation(tableIdentifier, dropPartitions.ifExists(), specs);
        } else if (sqlAlterTable instanceof SqlAlterTableAddWatermark) {
            SqlAlterTableAddWatermark alterTableAddWatermark =
                    (SqlAlterTableAddWatermark) sqlAlterTable;
            return convertAlterTableAddWatermark(alterTableAddWatermark);
        } else if (sqlAlterTable instanceof SqlAlterTableAddColumn) {
            SqlAlterTableAddColumn alterTableAddColumn = (SqlAlterTableAddColumn) sqlAlterTable;
            return convertAlterTableAddColumn(alterTableAddColumn);
        } else if (sqlAlterTable instanceof SqlAlterTableAddColumns) {
            SqlAlterTableAddColumns sqlAlterTableAddColumns =
                    (SqlAlterTableAddColumns) sqlAlterTable;
            return convertAlterTableAddColumns(sqlAlterTableAddColumns);
        } else if (sqlAlterTable instanceof SqlAlterTableCompact) {
            ResolvedCatalogTable resolvedCatalogTable =
                    optionalCatalogTable.get().getResolvedTable();
            return convertAlterTableCompact(
                    tableIdentifier, resolvedCatalogTable, (SqlAlterTableCompact) sqlAlterTable);
        } else {
            throw new ValidationException(
                    String.format(
                            "[%s] needs to implement",
                            sqlAlterTable.toSqlString(CalciteSqlDialect.DEFAULT)));
        }
    }

    private CatalogTable lookupTargetTable(SqlAlterTable sqlAlterTable) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterTable.getTableName().names);
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        ContextResolvedTable lookupResult =
                catalogManager
                        .getTable(identifier)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Target table '%s' of the ALTER clause not found in the catalog, at %s",
                                                        identifier,
                                                        sqlAlterTable
                                                                .getTableName()
                                                                .getParserPosition())));
        if (!(lookupResult.getTable() instanceof CatalogTable)) {
            throw new ValidationException(
                    String.format(
                            "Target table '%s' of the ALTER clause can not be a VIEW, at %s",
                            identifier, sqlAlterTable.getTableName().getParserPosition()));
        }
        return lookupResult.getTable();
    }

    private Operation convertAlterTableAddColumns(SqlAlterTableAddColumns sqlAlterTableAddColumns) {

        List<SqlNode> columnList = sqlAlterTableAddColumns.getColumnNodes();
        sqlAlterTableAddColumns.getFullConstraints().stream()
                .filter(SqlTableConstraint::isUnique)
                .findAny()
                .ifPresent(validateTableConstraint);
        Optional<SqlTableConstraint> primaryKey =
                sqlAlterTableAddColumns.getFullConstraints().stream()
                        .filter(SqlTableConstraint::isPrimaryKey)
                        .findAny();
        List<SqlWatermark> watermarks = sqlAlterTableAddColumns.getWatermarks();
        CatalogTable oriTable = lookupTargetTable(sqlAlterTableAddColumns);
        Map<SqlTableLike.FeatureOption, SqlTableLike.MergingStrategy> mergingStrategies =
                mergeTableLikeUtil.computeMergingStrategies(Collections.emptyList());
        TableSchema sourceTableSchema =
                TableSchema.fromResolvedSchema(
                        oriTable.getUnresolvedSchema().resolve(catalogManager.getSchemaResolver()));

        TableSchema mergedSchema =
                mergeTableLikeUtil.mergeTables(
                        mergingStrategies,
                        sourceTableSchema,
                        columnList,
                        watermarks,
                        primaryKey.orElse(null));
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterTableAddColumns.fullTableName());
        ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        return new AlterTableAddColumnsOperation(
                tableIdentifier,
                new CatalogTableImpl(
                        mergedSchema,
                        oriTable.getPartitionKeys(),
                        oriTable.getOptions(),
                        oriTable.getComment()),
                oriTable);
    }

    private Operation convertAlterTableAddColumn(SqlAlterTableAddColumn alterTableAddColumn) {
        List<SqlNode> columnList = Collections.singletonList(alterTableAddColumn.getColumnNode());
        CatalogTable oriTable = lookupTargetTable(alterTableAddColumn);
        alterTableAddColumn.getConstraint().ifPresent(validateTableConstraint);
        Map<SqlTableLike.FeatureOption, SqlTableLike.MergingStrategy> mergingStrategies =
                mergeTableLikeUtil.computeMergingStrategies(Collections.emptyList());
        TableSchema sourceTableSchema =
                TableSchema.fromResolvedSchema(
                        oriTable.getUnresolvedSchema().resolve(catalogManager.getSchemaResolver()));

        TableSchema mergedSchema =
                mergeTableLikeUtil.mergeTables(
                        mergingStrategies,
                        sourceTableSchema,
                        columnList,
                        Collections.emptyList(),
                        alterTableAddColumn.getConstraint().orElse(null));
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(alterTableAddColumn.fullTableName());
        ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        return new AlterTableAddColumnOperation(
                tableIdentifier,
                new CatalogTableImpl(
                        mergedSchema,
                        oriTable.getPartitionKeys(),
                        oriTable.getOptions(),
                        oriTable.getComment()),
                oriTable);
    }

    private Operation convertAlterTableAddWatermark(
            SqlAlterTableAddWatermark alterTableAddWatermark) {

        Preconditions.checkNotNull(
                alterTableAddWatermark.getWatermark(),
                "Watermark in alter table clause must not be null.");

        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(alterTableAddWatermark.fullTableName());
        ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        CatalogTable table = lookupTargetTable(alterTableAddWatermark);
        final TableSchema sourceTableSchema =
                TableSchema.fromResolvedSchema(
                        table.getUnresolvedSchema().resolve(catalogManager.getSchemaResolver()));
        List<WatermarkSpec> watermarkSpecs = sourceTableSchema.getWatermarkSpecs();
        if (!watermarkSpecs.isEmpty()) {
            throw new ValidationException(
                    String.format("There is a watermark '%s' already.", watermarkSpecs.get(0)));
        }
        // reuse the part of create ... like ... clause code.
        TableSchema mergedSchema =
                mergeTableLikeUtil.mergeTables(
                        Maps.newHashMap(),
                        sourceTableSchema,
                        Collections.emptyList(),
                        Collections.singletonList(alterTableAddWatermark.getWatermark()),
                        null);
        WatermarkSpec toAddWatermark = mergedSchema.getWatermarkSpecs().get(0);

        return new AlterTableAddWatermarkOperation(
                tableIdentifier,
                toAddWatermark.getRowtimeAttribute(),
                toAddWatermark.getWatermarkExpr(),
                toAddWatermark.getWatermarkExprOutputType());
    }

    private Operation convertAlterTableOptions(
            ObjectIdentifier tableIdentifier,
            CatalogTable oldTable,
            SqlAlterTableOptions alterTableOptions) {
        LinkedHashMap<String, String> partitionKVs = alterTableOptions.getPartitionKVs();
        // it's altering partitions
        if (partitionKVs != null) {
            CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(partitionKVs);
            CatalogPartition catalogPartition =
                    catalogManager
                            .getPartition(tableIdentifier, partitionSpec)
                            .orElseThrow(
                                    () ->
                                            new ValidationException(
                                                    String.format(
                                                            "Partition %s of table %s doesn't exist",
                                                            partitionSpec.getPartitionSpec(),
                                                            tableIdentifier)));
            Map<String, String> newProps = new HashMap<>(catalogPartition.getProperties());
            newProps.putAll(
                    OperationConverterUtils.extractProperties(alterTableOptions.getPropertyList()));
            return new AlterPartitionPropertiesOperation(
                    tableIdentifier,
                    partitionSpec,
                    new CatalogPartitionImpl(newProps, catalogPartition.getComment()));
        } else {
            // it's altering a table
            Map<String, String> newOptions = new HashMap<>(oldTable.getOptions());
            newOptions.putAll(
                    OperationConverterUtils.extractProperties(alterTableOptions.getPropertyList()));
            return new AlterTableOptionsOperation(tableIdentifier, oldTable.copy(newOptions));
        }
    }

    private Operation convertAlterTableReset(
            ObjectIdentifier tableIdentifier,
            CatalogTable oldTable,
            SqlAlterTableReset alterTableReset) {
        Map<String, String> newOptions = new HashMap<>(oldTable.getOptions());
        // reset empty or 'connector' key is not allowed
        Set<String> resetKeys = alterTableReset.getResetKeys();
        if (resetKeys.isEmpty() || resetKeys.contains(FactoryUtil.CONNECTOR.key())) {
            String exMsg =
                    resetKeys.isEmpty()
                            ? "ALTER TABLE RESET does not support empty key"
                            : "ALTER TABLE RESET does not support changing 'connector'";
            throw new ValidationException(exMsg);
        }
        // reset table option keys
        resetKeys.forEach(newOptions::remove);
        return new AlterTableOptionsOperation(tableIdentifier, oldTable.copy(newOptions));
    }

    private Operation convertAlterTableCompact(
            ObjectIdentifier tableIdentifier,
            ResolvedCatalogTable resolvedCatalogTable,
            SqlAlterTableCompact alterTableCompact) {
        Catalog catalog = catalogManager.getCatalog(tableIdentifier.getCatalogName()).orElse(null);
        if (ManagedTableListener.isManagedTable(catalog, resolvedCatalogTable)) {
            LinkedHashMap<String, String> partitionKVs = alterTableCompact.getPartitionKVs();
            CatalogPartitionSpec partitionSpec = null;
            if (partitionKVs != null) {
                List<String> orderedPartitionKeys = resolvedCatalogTable.getPartitionKeys();
                Set<String> validPartitionKeySet = new HashSet<>(orderedPartitionKeys);
                String exMsg =
                        orderedPartitionKeys.isEmpty()
                                ? String.format("Table %s is not partitioned.", tableIdentifier)
                                : String.format(
                                        "Available ordered partition columns: [%s]",
                                        orderedPartitionKeys.stream()
                                                .collect(Collectors.joining("', '", "'", "'")));
                partitionKVs.forEach(
                        (partitionKey, partitionValue) -> {
                            if (!validPartitionKeySet.contains(partitionKey)) {
                                throw new ValidationException(
                                        String.format(
                                                "Partition column '%s' not defined in the table schema. %s",
                                                partitionKey, exMsg));
                            }
                        });
                partitionSpec = new CatalogPartitionSpec(partitionKVs);
            }
            return new AlterTableCompactOperation(tableIdentifier, partitionSpec);
        }
        throw new ValidationException(
                String.format(
                        "ALTER TABLE COMPACT operation is not supported for non-managed table %s",
                        tableIdentifier));
    }
}
