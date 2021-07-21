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
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.util.Preconditions;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.URL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.USERNAME;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** Catalog for MySQL. */
public class MySQLCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLCatalog.class);

    private final String databaseVersion;
    private final String driverVersion;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            LOG.error("获取驱动失败");
        }
    }
    // ============================data types=====================

    public static final String MYSQL_UNKNOWN = "UNKNOWN";
    public static final String MYSQL_BIT = "BIT";

    // -------------------------number----------------------------
    public static final String MYSQL_TINYINT = "TINYINT";
    public static final String MYSQL_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    public static final String MYSQL_SMALLINT = "SMALLINT";
    public static final String MYSQL_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    public static final String MYSQL_MEDIUMINT = "MEDIUMINT";
    public static final String MYSQL_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    public static final String MYSQL_INT = "INT";
    public static final String MYSQL_INT_UNSIGNED = "INT UNSIGNED";
    public static final String MYSQL_INTEGER = "INTEGER";
    public static final String MYSQL_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    public static final String MYSQL_BIGINT = "BIGINT";
    public static final String MYSQL_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    public static final String MYSQL_DECIMAL = "DECIMAL";
    public static final String MYSQL_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    public static final String MYSQL_FLOAT = "FLOAT";
    public static final String MYSQL_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    public static final String MYSQL_DOUBLE = "DOUBLE";
    public static final String MYSQL_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";

    // -------------------------string----------------------------
    public static final String MYSQL_CHAR = "CHAR";
    public static final String MYSQL_VARCHAR = "VARCHAR";
    public static final String MYSQL_TINYTEXT = "TINYTEXT";
    public static final String MYSQL_MEDIUMTEXT = "MEDIUMTEXT";
    public static final String MYSQL_TEXT = "TEXT";
    public static final String MYSQL_LONGTEXT = "LONGTEXT";
    public static final String MYSQL_JSON = "JSON";

    // ------------------------------time-------------------------
    public static final String MYSQL_DATE = "DATE";
    public static final String MYSQL_DATETIME = "DATETIME";
    public static final String MYSQL_TIME = "TIME";
    public static final String MYSQL_TIMESTAMP = "TIMESTAMP";
    public static final String MYSQL_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    public static final String MYSQL_TINYBLOB = "TINYBLOB";
    public static final String MYSQL_MEDIUMBLOB = "MEDIUMBLOB";
    public static final String MYSQL_BLOB = "BLOB";
    public static final String MYSQL_LONGBLOB = "LONGBLOB";
    public static final String MYSQL_BINARY = "BINARY";
    public static final String MYSQL_VARBINARY = "VARBINARY";
    public static final String MYSQL_GEOMETRY = "GEOMETRY";

    // column class names
    public static final String COLUMN_CLASS_BOOLEAN = "java.lang.Boolean";
    public static final String COLUMN_CLASS_BYTE_ARRAY = "[B";
    public static final String COLUMN_CLASS_STRING = "java.lang.String";

    public static final int RAW_TIME_LENGTH = 10;
    public static final int RAW_TIMESTAMP_LENGTH = 19;

    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("information_schema");
                    add("mysql");
                    add("performance_schema");
                    add("sys");
                }
            };
    private static final Set<String> supportedDriverVersions =
            new HashSet<String>() {
                {
                    add("5");
                    add("6");
                    add("8.0");
                }
            };

    private static final Set<String> supportedMySQLVersions =
            new HashSet<String>() {
                {
                    add("5.7");
                    add("8.0");
                }
            };

    public MySQLCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
        LOG.info("debug: catalogName: {}, defaultDatabase: {}, username: {}, pwd: {}, baseUrl: {}",
                catalogName, defaultDatabase, username, pwd, baseUrl);
        this.driverVersion =
                Preconditions.checkNotNull(getDriverVersion(), "driver version must not be null.");
        this.databaseVersion =
                Preconditions.checkNotNull(
                        getDatabaseVersion(), "database version must not be null.");
        Preconditions.checkState(
                isSupportedVersion(supportedDriverVersions, driverVersion),
                "Doesn't support driver version '%s' yet.");
        Preconditions.checkState(
                isSupportedVersion(supportedMySQLVersions, databaseVersion),
                "Doesn't support mysql version '%s' yet.");
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
        try (Connection conn =
                DriverManager.getConnection(baseUrl + tablePath.getDatabaseName(), username, pwd)) {
            String sql = String.format("SELECT * FROM %s limit 1;", tablePath.getObjectName());
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSetMetaData resultSetMetaData = ps.getMetaData();
            String[] columnsClassnames = new String[resultSetMetaData.getColumnCount()];
            DataType[] types = new DataType[resultSetMetaData.getColumnCount()];
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                columnsClassnames[i - 1] = resultSetMetaData.getColumnName(i);
                types[i - 1] = fromJDBCType(tablePath, resultSetMetaData, i);
                if (resultSetMetaData.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                    types[i - 1] = types[i - 1].notNull();
                }
            }
            TableSchema.Builder tableBuilder =
                    new TableSchema.Builder().fields(columnsClassnames, types);
            getPrimaryKey(conn.getMetaData(), null, tablePath.getObjectName())
                    .ifPresent(
                            pk ->
                                    tableBuilder.primaryKey(
                                            pk.getName(), pk.getColumns().toArray(new String[0])));
            TableSchema tableSchema = tableBuilder.build();
            Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR.key(), IDENTIFIER);
            props.put(URL.key(), baseUrl + tablePath.getDatabaseName());
            props.put(TABLE_NAME.key(), tablePath.getObjectName());
            props.put(USERNAME.key(), username);
            props.put(PASSWORD.key(), pwd);
            LOG.info("_____{}", props);
            ps.close();
            return new CatalogTableImpl(tableSchema, props, "");
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            return listTables(tablePath.getDatabaseName()).contains(tablePath.getObjectName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
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

    private boolean isSupportedVersion(Set<String> supportedVersions, String version) {
        return Objects.nonNull(supportedVersions)
                && supportedVersions.stream().anyMatch(version::startsWith);
    }

    private String getDatabaseVersion() {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            return conn.getMetaData().getDatabaseProductVersion();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed in getting MySQL version by %s.", defaultUrl), e);
        }
    }

    private String getDriverVersion() {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            String driverVersion = conn.getMetaData().getDriverVersion();
            LOG.info("dep-roc: " + driverVersion);
            Pattern regexp = Pattern.compile("\\d*?\\.\\d*?\\.\\d*");
            Matcher matcher = regexp.matcher(driverVersion);
            return matcher.group();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed in getting mysql driver version by %s.", defaultUrl), e);
        }
    }

    private void release(AutoCloseable... autoCloseables) {
        if (Objects.nonNull(autoCloseables)) {
            Arrays.stream(autoCloseables)
                    .filter(Objects::nonNull)
                    .forEach(
                            autoCloseable -> {
                                try {
                                    autoCloseable.close();
                                } catch (Exception e) {
                                    throw new CatalogException(
                                            "Failed in releasing sql resource.", e);
                                }
                            });
        }
    }

    /** Converts MySQL type to Flink {@link DataType} * */
    private DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        String columnName = metadata.getColumnName(colIndex);

        switch (mysqlType) {
            case MYSQL_BIT:
                return DataTypes.BOOLEAN();

            case MYSQL_TINYINT:
                return DataTypes.TINYINT();
            case MYSQL_TINYINT_UNSIGNED:
            case MYSQL_SMALLINT:
                return DataTypes.SMALLINT();
            case MYSQL_SMALLINT_UNSIGNED:
            case MYSQL_MEDIUMINT:
            case MYSQL_MEDIUMINT_UNSIGNED:
            case MYSQL_INT:
            case MYSQL_INTEGER:
                return DataTypes.INT();
            case MYSQL_INT_UNSIGNED:
            case MYSQL_INTEGER_UNSIGNED:
            case MYSQL_BIGINT:
                return DataTypes.BIGINT();
            case MYSQL_BIGINT_UNSIGNED:
                return DataTypes.DECIMAL(20, 0);
            case MYSQL_DECIMAL:
                return DataTypes.DECIMAL(precision, scale);
            case MYSQL_DECIMAL_UNSIGNED:
                checkMaxPrecision(tablePath, columnName, precision);
                return DataTypes.DECIMAL(precision + 1, scale);
            case MYSQL_FLOAT:
                return DataTypes.FLOAT();
            case MYSQL_FLOAT_UNSIGNED:
                LOG.warn(
                        "Performing type conversion [from {} to {}] to prevent value overflow.",
                        MYSQL_FLOAT_UNSIGNED,
                        MYSQL_DOUBLE);
                return DataTypes.DOUBLE();
            case MYSQL_DOUBLE:
                return DataTypes.DOUBLE();
            case MYSQL_DOUBLE_UNSIGNED:
                LOG.warn("{} will probably case value overflow.", MYSQL_DOUBLE_UNSIGNED);
                return DataTypes.DOUBLE();

            case MYSQL_CHAR:
                return DataTypes.CHAR(precision);
            case MYSQL_VARCHAR:
            case MYSQL_TINYTEXT:
            case MYSQL_MEDIUMTEXT:
            case MYSQL_TEXT:
            case MYSQL_LONGTEXT:
            case MYSQL_JSON:
                return DataTypes.VARCHAR(precision);

            case MYSQL_YEAR:
            case MYSQL_DATE:
                return DataTypes.DATE();
            case MYSQL_TIME:
                return DataTypes.TIME(
                        precision > RAW_TIME_LENGTH ? precision - RAW_TIME_LENGTH - 1 : precision);
            case MYSQL_DATETIME:
            case MYSQL_TIMESTAMP:
                return DataTypes.TIMESTAMP(
                        precision > RAW_TIMESTAMP_LENGTH
                                ? precision - RAW_TIMESTAMP_LENGTH - 1
                                : precision);

            case MYSQL_TINYBLOB:
            case MYSQL_MEDIUMBLOB:
            case MYSQL_BLOB:
            case MYSQL_LONGBLOB:
            case MYSQL_GEOMETRY:
            case MYSQL_VARBINARY:
                return DataTypes.VARBINARY(precision);
            case MYSQL_BINARY:
                return DataTypes.BINARY(precision);
            case MYSQL_UNKNOWN:
                return fromJDBCClassType(tablePath, metadata, colIndex);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support mysql type '%s' in mysql version %s, driver version %s yet.",
                                mysqlType, databaseVersion, driverVersion));
        }
    }

    private void checkMaxPrecision(ObjectPath tablePath, String columnName, int precision) {
        if (precision >= DecimalType.MAX_PRECISION) {
            throw new CatalogException(
                    String.format(
                            "precision %s of table %s column name %s in type %s exceeds DecimalType.MAX_PRECISION %s.",
                            precision,
                            tablePath.getFullName(),
                            columnName,
                            MYSQL_DECIMAL_UNSIGNED,
                            DecimalType.MAX_PRECISION));
        }
    }

    /** Converts MySQL type to Flink {@link DataType} * */
    private DataType fromJDBCClassType(
            ObjectPath tablePath, ResultSetMetaData metadata, int colIndex) throws SQLException {
        final String jdbcColumnClassType = metadata.getColumnClassName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        LOG.warn(
                "Column {} in {} of mysql database, jdbcColumnClassType: {},"
                        + " jdbcColumnType: {}, precision: {}, scale: {},"
                        + " will use jdbc column class name to inference the type mapping.",
                metadata.getColumnName(colIndex),
                tablePath.getFullName(),
                jdbcColumnClassType,
                metadata.getColumnTypeName(colIndex),
                precision,
                scale);

        switch (jdbcColumnClassType) {
            case COLUMN_CLASS_BYTE_ARRAY:
                return DataTypes.VARBINARY(precision);
            case COLUMN_CLASS_STRING:
                return DataTypes.VARCHAR(precision);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support mysql column class type '%s' in mysql version %s, driver version %s yet.",
                                jdbcColumnClassType, databaseVersion, driverVersion));
        }
    }
}
