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

package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactoryOptions.USERNAME;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.URL;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** Catalog for MySQL. TODO: 关于两个switch无符号数需要仔细留意 */
public class MySQLCatalog extends AbstractJdbcCatalog {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLCatalog.class);

    // ============================data types=====================================================

    public static final String MYSQL_UNKNOWN = "UNKNOWN";

    /** t3 [B in driver5.X.X; Boolean in driver6/8 .0.X * */
    public static final String MYSQL_BIT = "BIT";

    /** number * */
    /** t36 java.lang.Integer * */
    public static final String MYSQL_TINYINT = "TINYINT";
    /** t36_1 java.lang.Integer * */
    public static final String MYSQL_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    /** t31 java.lang.Integer * */
    public static final String MYSQL_SMALLINT = "SMALLINT";
    /** t31_1 java.lang.Integer * */
    public static final String MYSQL_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    /** t21 java.lang.Integer * */
    public static final String MYSQL_MEDIUMINT = "MEDIUMINT";
    /** t21_1 java.lang.Integer * */
    public static final String MYSQL_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    /** t14 java.lang.Integer * */
    public static final String MYSQL_INT = "INT";
    /** t14_1 java.lang.Long * */
    public static final String MYSQL_INT_UNSIGNED = "INT UNSIGNED";
    /** t14 java.lang.Integer driver 5.X.X * */
    public static final String MYSQL_INTEGER = "INTEGER";
    /** t14_1 java.lang.Long driver 5.X.X * */
    public static final String MYSQL_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    /** t1_1 java.lang.Long * */
    public static final String MYSQL_BIGINT = "BIGINT";
    /** t1 java.math.BigInteger * */
    public static final String MYSQL_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    /** t8 java.math.BigDecimal * */
    public static final String MYSQL_DECIMAL = "DECIMAL";
    /** t8_1 java.math.BigDecimal * */
    public static final String MYSQL_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    /** t26 java.math.BigDecimal * */
    public static final String MYSQL_NUMERIC = MYSQL_DECIMAL;
    /** t26_1 java.math.BigDecimal * */
    public static final String MYSQL_NUMERIC_UNSIGNED = MYSQL_DECIMAL_UNSIGNED;
    /** t11 java.lang.Float * */
    public static final String MYSQL_FLOAT = "FLOAT";
    /** t11_1 java.lang.Float * */
    public static final String MYSQL_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    /** t9 java.lang.Double * */
    public static final String MYSQL_DOUBLE = "DOUBLE";
    /** t9_1 java.lang.Double * */
    public static final String MYSQL_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    /** t29 java.lang.Double * */
    public static final String MYSQL_REAL = MYSQL_DOUBLE;
    /** t29_1 java.lang.Double * */
    public static final String MYSQL_REAL_UNSIGNED = MYSQL_DOUBLE_UNSIGNED;

    // -------------------------string----------------------------------
    /** t5 java.lang.String * */
    public static final String MYSQL_CHAR = "CHAR";
    /** t10 java.lang.String 最大precision为最大元素的长度* */
    public static final String MYSQL_ENUM = MYSQL_CHAR;
    /** t30 java.lang.String * */
    public static final String MYSQL_SET = MYSQL_CHAR;
    /** t39 java.lang.String * */
    public static final String MYSQL_VARCHAR = "VARCHAR";
    /** t37 java.lang.String * */
    public static final String MYSQL_TINYTEXT = "TINYTEXT";
    /** t22 java.lang.String * */
    public static final String MYSQL_MEDIUMTEXT = "MEDIUMTEXT";
    /** t32 java.lang.String * */
    public static final String MYSQL_TEXT = "TEXT";
    /** t19 java.lang.String * */
    public static final String MYSQL_LONGTEXT = "LONGTEXT";
    /** t16 java.lang.String * */
    public static final String MYSQL_JSON = "JSON";

    // ------------------------------time------------------------------------
    /** t6 java.sql.Date * */
    public static final String MYSQL_DATE = "DATE";
    /** t7 java.sql.Timestamp * */
    public static final String MYSQL_DATETIME = "DATETIME";
    /** t33 java.sql.Time * */
    public static final String MYSQL_TIME = "TIME";
    /** t34 java.sql.Timestamp * */
    public static final String MYSQL_TIMESTAMP = "TIMESTAMP";
    /** t40 java.sql.Date * */
    public static final String MYSQL_YEAR = "YEAR";

    // ------------------------------blob------------------------------------
    /** t35 [B * */
    public static final String MYSQL_TINYBLOB = "TINYBLOB";
    /** t20 [B * */
    public static final String MYSQL_MEDIUMBLOB = "MEDIUMBLOB";
    /** t4 [B * */
    public static final String MYSQL_BLOB = "BLOB";
    /** t18 [B * */
    public static final String MYSQL_LONGBLOB = "LONGBLOB";
    /** t2 [B * */
    public static final String MYSQL_BINARY = "BINARY";
    /** t38 [B * */
    public static final String MYSQL_VARBINARY = "VARBINARY";
    /** t12 [B * */
    public static final String MYSQL_GEOMETRY = "GEOMETRY";
    /** t13 [B in mysql5.7X * */
    public static final String MYSQL_GEOMETRY_COLLECTION = MYSQL_GEOMETRY;
    /** t13 [B in mysql8 * */
    public static final String MYSQL_GEOM_COLLECTION = MYSQL_GEOMETRY;
    /** t17 [B * */
    public static final String MYSQL_LINE_STRING = MYSQL_GEOMETRY;
    /** t23 [B * */
    public static final String MYSQL_MULTI_LINE_STRING = MYSQL_GEOMETRY;
    /** t24 [B * */
    public static final String MYSQL_MULTI_POINT = MYSQL_GEOMETRY;
    /** t25 [B * */
    public static final String MYSQL_MULTI_POLYGON = MYSQL_GEOMETRY;
    /** t27 [B * */
    public static final String MYSQL_POINT = MYSQL_GEOMETRY;
    /** t28 [B * */
    public static final String MYSQL_POLYGON = MYSQL_GEOMETRY;

    // column class names
    public static final String COLUMN_CLASS_BOOLEAN = "java.lang.Boolean";
    public static final String COLUMN_CLASS_INTEGER = "java.lang.Integer";
    public static final String COLUMN_CLASS_BIG_INTEGER = "java.math.BigInteger";
    public static final String COLUMN_CLASS_LONG = "java.lang.Long";
    public static final String COLUMN_CLASS_FLOAT = "java.lang.Float";
    public static final String COLUMN_CLASS_DOUBLE = "java.lang.Double";
    public static final String COLUMN_CLASS_BIG_DECIMAL = "java.math.BigDecimal";
    public static final String COLUMN_CLASS_BYTE_ARRAY = "[B";
    public static final String COLUMN_CLASS_STRING = "java.lang.String";
    public static final String COLUMN_CLASS_DATE = "java.sql.Date";
    public static final String COLUMN_CLASS_TIME = "java.sql.Time";
    public static final String COLUMN_CLASS_TIMESTAMP = "java.sql.Timestamp";

    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("information_schema");
                    add("mysql");
                    add("performance_schema");
                    add("sys");
                }
            };

    public MySQLCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        String sql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA;";
        return extractColumnValuesBySQL(
                defaultUrl,
                sql,
                1,
                (FilterFunction<String>) dbName -> !builtinDatabases.contains(dbName));
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        String connUrl = baseUrl + databaseName;
        String sql =
                String.format(
                        "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = %s",
                        databaseName);
        return extractColumnValuesBySQL(connUrl, sql, 1, null);
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String dbUrl = baseUrl + tablePath.getDatabaseName();
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();

            // MySQL 没有 schema 概念，直接传 null
            Optional<UniqueConstraint> primaryKey =
                    getPrimaryKey(metaData, null, tablePath.getObjectName());

            PreparedStatement ps =
                    conn.prepareStatement(
                            String.format("SELECT * FROM %s limit 1;", tablePath.getObjectName()));

            ResultSetMetaData rsmd = ps.getMetaData();

            // 列名称
            String[] columnsClassnames = new String[rsmd.getColumnCount()];
            // 列类型(flink中)
            DataType[] types = new DataType[rsmd.getColumnCount()];

            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                columnsClassnames[i - 1] = rsmd.getColumnName(i);
                types[i - 1] = fromJDBCType(rsmd, i);
                if (rsmd.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                    types[i - 1] = types[i - 1].notNull();
                }
            }

            TableSchema.Builder tableBuilder =
                    new TableSchema.Builder().fields(columnsClassnames, types);
            primaryKey.ifPresent(
                    pk ->
                            tableBuilder.primaryKey(
                                    pk.getName(), pk.getColumns().toArray(new String[0])));

            TableSchema tableSchema = tableBuilder.build();

            Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR.key(), IDENTIFIER);
            props.put(URL.key(), dbUrl);
            props.put(TABLE_NAME.key(), tablePath.getObjectName());
            props.put(USERNAME.key(), username);
            props.put(PASSWORD.key(), pwd);
            // 返回 CatalogTableImpl 与 create table sql 所做的事情是一致的
            return new CatalogTableImpl(tableSchema, props, "");
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        List<String> tables = null;
        try {
            tables = listTables(tablePath.getDatabaseName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
        return tables.contains(tablePath.getObjectName());
    }

    private List<String> extractColumnValuesBySQL(
            String connUrl, String sql, int columnIndex, FilterFunction<String> filterFunc) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<String> columnValues = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(connUrl, username, pwd)) {
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                String columnValue = rs.getString(columnIndex);
                if (Objects.isNull(filterFunc) || filterFunc.filter(columnValue)) {
                    columnValues.add(columnValue);
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed in processing query sql %s, connUrl %s", sql, connUrl),
                    e);
        } finally {
            release(rs, ps);
        }
    }

    private void release(AutoCloseable... autoCloseables) {
        if (Objects.nonNull(autoCloseables)) {
            for (AutoCloseable autoCloseable : autoCloseables) {
                if (Objects.nonNull(autoCloseable)) {
                    try {
                        autoCloseable.close();
                    } catch (Exception e) {
                        throw new CatalogException("Failed in releasing sql resource.", e);
                    }
                }
            }
        }
    }

    @Deprecated
    /** SELECT @@VERSION. * */
    private Optional<String> queryMySQLVersion() {
        List<String> versions = extractColumnValuesBySQL(defaultUrl, "SELECT VERSION();", 1, null);
        return Objects.nonNull(versions) && !versions.isEmpty()
                ? Optional.of(versions.get(0))
                : Optional.empty();
    }

    /** Converts MySQL type to Flink {@link DataType} * */
    private DataType fromJDBCType(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String mysqlType =
                Preconditions.checkNotNull(metadata.getColumnTypeName(colIndex)).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        switch (mysqlType) {
            case MYSQL_UNKNOWN:
            case MYSQL_BIT:
                // 由于版本差异
                return fromJDBCClassType(metadata, colIndex);
                // ----------number------------
            case MYSQL_TINYINT:
                return DataTypes.TINYINT();
            case MYSQL_TINYINT_UNSIGNED:
                return DataTypes.SMALLINT();
            case MYSQL_SMALLINT:
                return DataTypes.SMALLINT();
            case MYSQL_SMALLINT_UNSIGNED:
                return DataTypes.INT();
            case MYSQL_MEDIUMINT:
                return DataTypes.INT();
            case MYSQL_MEDIUMINT_UNSIGNED:
                return DataTypes.INT();
            case MYSQL_INT:
            case MYSQL_INTEGER:
                return DataTypes.INT();
            case MYSQL_INT_UNSIGNED:
            case MYSQL_INTEGER_UNSIGNED:
                return DataTypes.BIGINT();
            case MYSQL_BIGINT:
                return DataTypes.BIGINT();
            case MYSQL_BIGINT_UNSIGNED:
                return DataTypes.DECIMAL(20, 0);
            case MYSQL_DECIMAL:
                return DataTypes.DECIMAL(precision, scale);
            case MYSQL_DECIMAL_UNSIGNED:
                // TODO 溢出警告
                return DataTypes.DECIMAL(precision, scale);
            case MYSQL_FLOAT:
                return DataTypes.FLOAT();
            case MYSQL_FLOAT_UNSIGNED:
                // TODO 溢出警告
                return DataTypes.FLOAT();
            case MYSQL_DOUBLE:
                return DataTypes.DOUBLE();
            case MYSQL_DOUBLE_UNSIGNED:
                // TODO 溢出警告
                return DataTypes.DOUBLE();
                // -------string
            case MYSQL_CHAR:
                return DataTypes.CHAR(precision);
            case MYSQL_VARCHAR:
            case MYSQL_TINYTEXT:
            case MYSQL_MEDIUMTEXT:
            case MYSQL_TEXT:
            case MYSQL_LONGTEXT:
            case MYSQL_JSON:
                return DataTypes.VARCHAR(precision);
                // -------time---------
            case MYSQL_YEAR: // TODO
            case MYSQL_DATE:
                return DataTypes.DATE();
            case MYSQL_TIME:
                return DataTypes.TIME(scale);
            case MYSQL_DATETIME:
            case MYSQL_TIMESTAMP:
                return DataTypes.TIMESTAMP(scale);
                // -------blob---------
            case MYSQL_TINYBLOB:
            case MYSQL_MEDIUMBLOB:
            case MYSQL_BLOB:
            case MYSQL_LONGBLOB:
            case MYSQL_GEOMETRY:
            case MYSQL_VARBINARY:
                return DataTypes.VARBINARY(precision);
            case MYSQL_BINARY:
                return DataTypes.BINARY(precision);
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support mysql type '%s' yet.", mysqlType));
        }
    }

    /** Converts MySQL type to Flink {@link DataType} * */
    private DataType fromJDBCClassType(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        // TODO 进行退化的声明警告
        final String jdbcColumnClassType = metadata.getColumnClassName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        switch (jdbcColumnClassType) {
            case COLUMN_CLASS_BOOLEAN:
                return DataTypes.BOOLEAN();
            case COLUMN_CLASS_INTEGER:
                return DataTypes.INT();
            case COLUMN_CLASS_BIG_INTEGER:
                // TODO 需要声明告警的无符号问题
                return DataTypes.DECIMAL(precision + 1, 0);
            case COLUMN_CLASS_LONG:
                return DataTypes.BIGINT();
            case COLUMN_CLASS_FLOAT:
                return DataTypes.FLOAT();
            case COLUMN_CLASS_DOUBLE:
                return DataTypes.DOUBLE();
            case COLUMN_CLASS_BIG_DECIMAL:
                // TODO 需要声明告警的无符号问题
                if (precision == 65) {
                    return DataTypes.DECIMAL(precision, scale);
                } else {
                    return DataTypes.DECIMAL(precision + 1, scale);
                }
            case COLUMN_CLASS_BYTE_ARRAY:
                return DataTypes.BYTES();
            case COLUMN_CLASS_STRING:
                return DataTypes.VARCHAR(precision);
            case COLUMN_CLASS_DATE:
                return DataTypes.DATE();
            case COLUMN_CLASS_TIME:
                return DataTypes.TIME(precision > 10 ? precision - 11 : precision);
            case COLUMN_CLASS_TIMESTAMP:
                return DataTypes.TIMESTAMP(precision > 19 ? precision - 20 : precision);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support mysql column class type '%s' yet.",
                                jdbcColumnClassType));
        }
    }
}
