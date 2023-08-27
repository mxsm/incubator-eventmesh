/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eventmesh.connector.jdbc.table.catalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultTableEditorImpl implements TableEditor {

    private TableId tableId;

    private PrimaryKey primaryKey;

    private List<UniqueKey> uniqueKeys = new ArrayList<>(8);

    private Map<String, Column> columns = new HashMap<>(16);

    private String comment;

    private String sql;

    public DefaultTableEditorImpl(TableId tableId) {
        this.tableId = tableId;
    }

    @Override
    public TableId ofTableId() {
        return tableId;
    }

    @Override
    public List<Column> columns() {
        return columns.values().stream().collect(Collectors.toList());
    }

    @Override
    public Column columnWithName(String name) {
        return columns.get(name);
    }

    @Override
    public List<String> primaryKeyColumnNames() {
        return primaryKey == null ? null : primaryKey.getColumnNames();
    }

    /**
     * Returns a list of column that make up the primary key of the table.
     *
     * @return The list of primary key column names.
     */
    @Override
    public List<PrimaryKey> primaryKeyColumns() {
        return Arrays.asList(primaryKey);
    }

    @Override
    public TableEditor addColumns(Column... columns) {
        if (columns != null && columns.length > 0) {
            for (Column column : columns) {
                this.columns.put(column.getName(), column);
            }
        }
        return this;
    }

    @Override
    public TableEditor removeColumn(String columnName) {
        this.columns.remove(columnName);
        return this;
    }

    @Override
    public TableEditor setPrimaryKeyNames(List<String> pkColumnNames, String comment) {
        this.primaryKey = new PrimaryKey(pkColumnNames, comment);
        return this;
    }

    /**
     * Specifies the unique key columns for the table.
     *
     * @param ukColumnNames The names of the columns that form the unique key.
     * @param comment       The comment for the unique key constraint.
     * @return The updated TableEditor.
     */
    @Override
    public TableEditor setUniqueKeyColumnsNames(String ukName, List<String> ukColumnNames, String comment) {
        this.uniqueKeys.add(new UniqueKey(ukName, ukColumnNames, comment));
        return this;
    }

    @Override
    public TableEditor withComment(String comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public TableSchema create() {
        TableSchema.newTableSchemaBuilder()
            .withName(this.tableId.getTableName())
            .withColumns(new ArrayList<>(columns.values()))
            .withPrimaryKey(primaryKey)
            .withUniqueKeys(uniqueKeys)
            .withComment(comment)
            .build();
        return new TableSchema();
    }

    @Override
    public String ofSql() {
        return sql;
    }

    @Override
    public TableEditor withSql(String sql) {
        this.sql = sql;
        return this;
    }
}
