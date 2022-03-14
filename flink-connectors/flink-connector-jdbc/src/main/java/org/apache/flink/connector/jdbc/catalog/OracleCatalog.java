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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectTypeMapper;
import org.apache.flink.connector.jdbc.dialect.oracle.OracleTypeMapper;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Oracle Catalog Class.*/
@Internal
public class OracleCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(OracleCatalog.class);

    private final JdbcDialectTypeMapper dialectTypeMapper;

    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("information_schema");
                }
            };


    public OracleCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
        this.dialectTypeMapper = new OracleTypeMapper();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return null;
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return false;
    }
}
