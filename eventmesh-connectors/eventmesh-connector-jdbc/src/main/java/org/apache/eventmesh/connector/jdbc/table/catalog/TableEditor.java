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


import java.util.List;
import java.util.stream.Collectors;

/**
 * Edit and configure a database table schema.
 */
public interface TableEditor {

    String ofSql();

    /**
     * Returns the TableId associated with this table.
     *
     * @return The TableId of the table.
     */
    TableId ofTableId();

    /**
     * Returns a list of columns in the table.
     *
     * @return The list of columns in the table.
     */
    List<Column> columns();

    /**
     * Returns a list of column names in the table.
     *
     * @return The list of column names in the table.
     */
    default List<String> columnNames() {
        return columns().stream().map(Column::getName).collect(Collectors.toList());
    }

    /**
     * Returns the Column object with the given name.
     *
     * @param name The name of the column to retrieve.
     * @return The Column object with the given name, or null if not found.
     */
    Column columnWithName(String name);

    /**
     * Returns a list of column names that make up the primary key of the table.
     *
     * @return The list of primary key column names.
     */
    List<String> primaryKeyColumnNames();

    /**
     * Returns a list of column that make up the primary key of the table.
     *
     * @return The list of primary key column names.
     */
    List<PrimaryKey> primaryKeyColumns();

    /**
     * Returns true if the table has a primary key, false otherwise.
     *
     * @return True if the table has a primary key, false otherwise.
     */
    default boolean hasPrimaryKey() {
        return !primaryKeyColumnNames().isEmpty();
    }

    /**
     * Adds the specified columns to the table schema.
     *
     * @param columns The columns to be added to the table.
     * @return The updated TableEditor with the added columns.
     */
    TableEditor addColumns(Column... columns);

    /**
     * Removes the column with the given column name from the table schema.
     *
     * @param columnName The name of the column to be removed.
     * @return The updated TableEditor with the specified column removed.
     */
    TableEditor removeColumn(String columnName);

    /**
     * Sets the names of the columns that form the primary key of the table.
     *
     * @param pkColumnNames The list of column names that form the primary key.
     * @param comment       The comment for the primary key.
     * @return The updated TableEditor with the new primary key information.
     */
    TableEditor setPrimaryKeyNames(List<String> pkColumnNames, String comment);


    /**
     * Sets the unique key information for the table.
     *
     * @param ukName        The name of the unique key.
     * @param ukColumnNames The list of column names that form the unique key.
     * @param comment       The comment for the unique key.
     * @return The updated TableEditor with the new unique key information.
     */
    TableEditor setUniqueKeyColumnsNames(String ukName, List<String> ukColumnNames, String comment);


    /**
     * Sets the comment for the table schema.
     *
     * @param comment The comment to be set for the table.
     * @return The updated TableEditor with the new comment set.
     */
    TableEditor withComment(String comment);


    TableEditor withSql(String sql);

    /**
     * Creates and returns the final TableSchema based on the configured table properties.
     *
     * @return The TableSchema representing the final table configuration.
     */
    TableSchema create();
}

