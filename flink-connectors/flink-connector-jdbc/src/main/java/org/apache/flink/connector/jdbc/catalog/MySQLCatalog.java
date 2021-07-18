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

/** Catalog for MySQL. */
public class MySQLCatalog extends AbstractJdbcCatalog {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLCatalog.class);

    private static final String MYSQL_VERSION_5_7_PREFIX = "5.7";
    private static final String MYSQL_VERSION_8_PREFIX = "8.";

    //驱动只支持6.0X & 8.0X系列

    private static final Set<String> builtinDatabases = new HashSet<String>() {
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
        return extractColumnValuesBySQL(defaultUrl, sql, 1,
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
        String sql = String.format(
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
                            String.format("SELECT * FROM %s;", tablePath.getObjectName()));

            ResultSetMetaData rsmd = ps.getMetaData();

            // 列名称
            String[] names = new String[rsmd.getColumnCount()];
            // 列类型(flink中)
            DataType[] types = new DataType[rsmd.getColumnCount()];

            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                names[i - 1] = rsmd.getColumnName(i);
                types[i - 1] = fromJDBCType(rsmd, i);
                if (rsmd.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                    types[i - 1] = types[i - 1].notNull();
                }
            }

            TableSchema.Builder tableBuilder = new TableSchema.Builder().fields(names, types);
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
            String connUrl,
            String sql,
            int columnIndex,
            FilterFunction<String> filterFunc) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<String> columnValues = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(connUrl, username, pwd)) {
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                String columnValue = rs.getString(columnIndex);
                if (!(Objects.nonNull(filterFunc) && !filterFunc.filter(columnValue))) {
                    columnValues.add(columnValue);
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed in processing query sql %s, connUrl %s", sql, connUrl), e);
        } finally {
            release(rs, ps);
        }
    }

    /** Converts MySQL type to Flink {@link DataType} **/
    private DataType fromJDBCType(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex);

        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        switch (mysqlType) {
            case MYSQL_TINYINT:
                if (1 == precision) {
                    return DataTypes.BOOLEAN();
                }
                return DataTypes.TINYINT();
            case MYSQL_SMALLINT:
                return DataTypes.SMALLINT();
            case MYSQL_TINYINT_UNSIGNED:
                return DataTypes.SMALLINT();
            case MYSQL_INT:
                return DataTypes.INT();
            case MYSQL_MEDIUMINT:
                return DataTypes.INT();
            case MYSQL_SMALLINT_UNSIGNED:
                return DataTypes.INT();
            case MYSQL_BIGINT:
                return DataTypes.BIGINT();
            case MYSQL_INT_UNSIGNED:
                return DataTypes.BIGINT();
            case MYSQL_BIGINT_UNSIGNED:
                return DataTypes.DECIMAL(20, 0);
            case MYSQL_FLOAT:
                return DataTypes.FLOAT();
            case MYSQL_DOUBLE:
                return DataTypes.DOUBLE();
            case MYSQL_NUMERIC:
                return DataTypes.DECIMAL(precision, scale);
            case MYSQL_DECIMAL:
                return DataTypes.DECIMAL(precision, scale);
            case MYSQL_BIT:
                return DataTypes.BOOLEAN();
            case MYSQL_DATE:
                return DataTypes.DATE();
            case MYSQL_TIME:
                return DataTypes.TIME(scale);
            case MYSQL_DATETIME:
                return DataTypes.TIMESTAMP(scale);
            case MYSQL_CHAR:
                return DataTypes.CHAR(precision);
            case MYSQL_VARCHAR:
                return DataTypes.CHAR(precision);
            case MYSQL_TEXT:
                return DataTypes.STRING();
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support mysql type '%s' yet.", mysqlType));
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

    /** SELECT @@VERSION. **/
    private Optional<String> queryMySQLVersion() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection connection = DriverManager.getConnection(defaultUrl, username, pwd)) {
            ps = connection.prepareStatement("SELECT VERSION();");
            rs = ps.executeQuery();
            while (rs.next()) {
                return Optional.of(rs.getString(1));
            }
            return Optional.empty();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed in listing database in catalog %s", getName()), e);
        } finally {
            release(rs, ps);
        }
    }

    //----------------------------driver 5.X.X---------------------------------------------------

    public static final String MYSQL_BIT_50X =  "BIT";//t3 [B  driver-8

    public static final String MYSQL_UNKNOWN_ =  "UNKNOWN";//t12 [B  driver-8  GEOMETRY
    public static final String MYSQL_UNKNOWN =  "UNKNOWN";//t13 [B  driver-8  GEOMETRY
    public static final String MYSQL_INTEGER =  "INTEGER";//t14 java.lang.Integer  driver-8  INT
    public static final String MYSQL_INTEGER_UNSIGNED =  "INTEGER UNSIGNED";//t14_1 java.lang.Long  driver-8 INT UNSIGNED
    public static final String MYSQL_INTEGER =  "INTEGER";//t15 java.lang.Integer  driver-8 INT
    public static final String MYSQL_INTEGER_UNSIGNED =  "INTEGER UNSIGNED";//t15_1 java.lang.Long  driver-8 INT UNSIGNED
    public static final String MYSQL_UNKNOWN =  "UNKNOWN";//t16 java.lang.String  driver-8 JSON
    public static final String MYSQL_UNKNOWN =  "UNKNOWN";//t17 [B  driver-8 GEOMETRY
    public static final String MYSQL_VARCHAR =  "VARCHAR";//t19 java.lang.String  driver-8 LONGTEXT
    final String MYSQL_VARCHAR =  "VARCHAR";//t22 java.lang.String  driver-8 MEDIUMTEXT
    public static final String MYSQL_UNKNOWN =  "UNKNOWN";//t23 [B  driver-8 GEOMETRY
    public static final String MYSQL_UNKNOWN =  "UNKNOWN";//t24 [B  driver-8 GEOMETRY
    public static final String MYSQL_UNKNOWN =  "UNKNOWN";//t25 [B  driver-8 GEOMETRY
    final String MYSQL_UNKNOWN =  "UNKNOWN";//t27 [B  driver-8 GEOMETRY
    public static final String MYSQL_UNKNOWN =  "UNKNOWN";//t28 [B  driver-8 GEOMETRY
    public static final String MYSQL_VARCHAR =  "VARCHAR";//t32 java.lang.String  driver-8 TEXT
    public static final String MYSQL_VARCHAR =  "VARCHAR";//t37 java.lang.String  driver-8 TINYTEXT




    //----------------------------driver 8.0.X & 6.6.X-------------------------------------------

    /** number **/
    public static final String MYSQL_BIGINT_UNSIGNED = "BIGINT UNSIGNED";//t1 java.math.BigInteger
    public static final String MYSQL_BIGINT = "BIGINT";//t1_1 java.lang.Long
    //t8_1 java.math.BigDecimal
    public static final String MYSQL_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    public static final String MYSQL_DOUBLE = "DOUBLE";//t9 java.lang.Double
    public static final String MYSQL_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";//t9_1 java.lang.Double
    public static final String MYSQL_FLOAT = "FLOAT";//t11 java.lang.Float
    public static final String MYSQL_FLOAT_UNSIGNED = "FLOAT UNSIGNED";//t11_1 java.lang.Float
    public static final String MYSQL_DECIMAL = "DECIMAL";//t8 java.math.BigDecimal
    public static final String MYSQL_INT = "INT";//t14 java.lang.Integer
    public static final String MYSQL_INT_UNSIGNED = "INT UNSIGNED";//t14_1 java.lang.Long
    public static final String MYSQL_INTEGER = "INT";//t15 java.lang.Integer
    public static final String MYSQL_INTEGER_UNSIGNED = "INT UNSIGNED";//t15_1 java.lang.Long
    public static final String MYSQL_MEDIUMINT = "MEDIUMINT";//t21 java.lang.Integer
    //t21_1 java.lang.Integer
    public static final String MYSQL_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    //t26_1 java.math.BigDecimal
    public static final String MYSQL_NUMERIC_UNSIGNED = "DECIMAL UNSIGNED";
    public static final String MYSQL_NUMERIC = "DECIMAL";//t26 java.math.BigDecimal
    public static final String MYSQL_REAL = "DOUBLE";//t29 java.lang.Double
    public static final String MYSQL_REAL_UNSIGNED = "DOUBLE UNSIGNED";//t29_1 java.lang.Double
    public static final String MYSQL_SMALLINT = "SMALLINT";//t31 java.lang.Integer
    //t31_1 java.lang.Integer
    public static final String MYSQL_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    public static final String MYSQL_TINYINT = "TINYINT";//t36 java.lang.Integer
    public static final String MYSQL_TINYINT_UNSIGNED = "TINYINT UNSIGNED";//t36_1 java.lang.Integer

    /** string **/
    public static final String MYSQL_CHAR = "CHAR";//t5 java.lang.String
    public static final String MYSQL_ENUM = "CHAR";//t10 java.lang.String
    public static final String MYSQL_JSON = "JSON";//t16 java.lang.String
    public static final String MYSQL_LONGTEXT = "LONGTEXT";//t19 java.lang.String
    public static final String MYSQL_SET = "CHAR";//t30 java.lang.String
    public static final String MYSQL_MEDIUMTEXT = "MEDIUMTEXT";//t22 java.lang.String
    public static final String MYSQL_TEXT = "TEXT";//t32 java.lang.String
    public static final String MYSQL_VARCHAR = "VARCHAR";//t39 java.lang.String
    public static final String MYSQL_TINYTEXT = "TINYTEXT";//t37 java.lang.String

    /** time **/
    public static final String MYSQL_DATE = "DATE";//t6 java.sql.Date
    public static final String MYSQL_DATETIME = "DATETIME";//t7 java.sql.Timestamp
    public static final String MYSQL_TIME = "TIME";//t33 java.sql.Time
    public static final String MYSQL_TIMESTAMP = "TIMESTAMP";//t34 java.sql.Timestamp
    public static final String MYSQL_YEAR = "YEAR";//t40 java.sql.Date

    /** bit/boolean **/
    public static final String MYSQL_BIT = "BIT";//t3 java.lang.Boolean

    /** blob **/
    public static final String MYSQL_BLOB = "BLOB";//t4 [B
    public static final String MYSQL_BINARY = "BINARY";//t2 [B
    public static final String MYSQL_GEOMETRY = "GEOMETRY";//t12 [B
    // in mysql5.7X
    public static final String MYSQL_GEOMETRY_COLLECTION = "GEOMETRY";//t13 [B
    // in mysql8
    public static final String MYSQL_GEOM_COLLECTION = "GEOMETRY";//t13 [B
    public static final String MYSQL_LINE_STRING = "GEOMETRY";//t17 [B
    public static final String MYSQL_LONGBLOB = "LONGBLOB";//t18 [B
    public static final String MYSQL_MEDIUMBLOB = "MEDIUMBLOB";//t20 [B
    public static final String MYSQL_MULTI_LINE_STRING = "GEOMETRY";//t23 [B
    public static final String MYSQL_MULTI_POINT = "GEOMETRY";//t24 [B
    public static final String MYSQL_MULTI_POLYGON = "GEOMETRY";//t25 [B
    public static final String MYSQL_POINT = "GEOMETRY";//t27 [B
    public static final String MYSQL_POLYGON = "GEOMETRY";//t28 [B
    public static final String MYSQL_TINYBLOB = "TINYBLOB";//t35 [B
    public static final String MYSQL_VARBINARY = "VARBINARY";//t38 [B

}
