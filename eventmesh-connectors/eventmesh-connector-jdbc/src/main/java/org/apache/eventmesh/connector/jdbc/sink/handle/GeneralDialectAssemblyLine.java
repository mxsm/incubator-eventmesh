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

package org.apache.eventmesh.connector.jdbc.sink.handle;

import org.apache.eventmesh.common.utils.LogUtils;
import org.apache.eventmesh.connector.jdbc.CatalogChanges;
import org.apache.eventmesh.connector.jdbc.Field;
import org.apache.eventmesh.connector.jdbc.Schema;
import org.apache.eventmesh.connector.jdbc.dialect.AbstractGeneralDatabaseDialect;
import org.apache.eventmesh.connector.jdbc.dialect.DatabaseDialect;
import org.apache.eventmesh.connector.jdbc.dialect.DatabaseType;
import org.apache.eventmesh.connector.jdbc.dialect.SqlStatementAssembler;
import org.apache.eventmesh.connector.jdbc.event.SchemaChangeEventType;
import org.apache.eventmesh.connector.jdbc.source.SourceMateData;
import org.apache.eventmesh.connector.jdbc.table.catalog.Column;
import org.apache.eventmesh.connector.jdbc.table.catalog.Table;
import org.apache.eventmesh.connector.jdbc.table.catalog.TableId;
import org.apache.eventmesh.connector.jdbc.type.Type;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.hibernate.dialect.Dialect;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GeneralDialectAssemblyLine implements DialectAssemblyLine {

    private final DatabaseDialect databaseDialect;

    private final Dialect hibernateDialect;

    public GeneralDialectAssemblyLine(DatabaseDialect databaseDialect, Dialect hibernateDialect) {
        this.databaseDialect = databaseDialect;
        this.hibernateDialect = hibernateDialect;
    }

    /**
     * @return
     */
    @Override
    public String getDatabaseOrTableStatement(SourceMateData sourceMateData, CatalogChanges catalogChanges, String statement) {

        DatabaseType sourceDbType = DatabaseType.ofValue(sourceMateData.getConnector());
        //No need to perform statement-related conversions for database operations
        // that maintain consistency between the Source and Sink databases.
/*        if (sourceDbType == databaseDialect.getDatabaseType()) {
            return statement;
        }*/
        String type = catalogChanges.getType();
        String operationType = catalogChanges.getOperationType();
        SchemaChangeEventType schemaChangeEventType = SchemaChangeEventType.ofSchemaChangeEventType(type, operationType);
        String sql = null;
        switch (schemaChangeEventType) {
            case DATABASE_CREATE:
                sql = assembleCreateDatabaseSql(catalogChanges);
                break;
            case DATABASE_DROP:
                sql = assembleDropDatabaseSql(catalogChanges);
                break;
            case DATABASE_ALERT:
                sql = assembleAlertDatabaseSql(catalogChanges);
                break;
            case TABLE_CREATE:
                sql = assembleCreateTableSql(catalogChanges);
                break;
            case TABLE_DROP:
                sql = assembleDropTableSql(catalogChanges);
                break;
            case TABLE_ALERT:
                sql = assembleAlertTableSql(catalogChanges);
                break;
            default:
                LogUtils.warn(log, "Type={}, OperationType={} not support", type, operationType);
        }
        return sql;
    }

    /**
     * @return
     */
    @Override
    public String getUpsertStatement() {
        return null;
    }

    /**
     * @return
     */
    @Override
    public String getDeleteStatement() {
        return null;
    }

    /**
     * @return
     */
    @Override
    public String getUpdateStatement() {
        return null;
    }

    /**
     * @return
     */
    @Override
    public String getInsertStatement(SourceMateData sourceMateData, Schema schema, String originStatement) {
        TableId tableId = new TableId(sourceMateData.getCatalogName(), sourceMateData.getSchemaName(), sourceMateData.getTableName());

        List<Field> afterFields =
            schema.getFields().stream().filter(field -> StringUtils.equals(field.getField(), "after")).collect(Collectors.toList());

        SqlStatementAssembler sqlAssembler = new SqlStatementAssembler();
        sqlAssembler.appendSqlSlice("INSERT INTO ");
        sqlAssembler.appendSqlSliceLists(((AbstractGeneralDatabaseDialect<?, ?>) databaseDialect).getQualifiedTableName(tableId));
        sqlAssembler.appendSqlSlice(" (");
        // assemble columns
        Field afterField = afterFields.get(0);
        List<Column<?>> columns = afterField.getFields().stream().map(item -> item.getColumn()).collect(Collectors.toList());
        sqlAssembler.appendSqlSliceOfColumns(", ", columns, column -> column.getName());
        sqlAssembler.appendSqlSlice(") VALUES (");
        //assemble values
        
        sqlAssembler.appendSqlSliceOfColumns(", ", columns,
            column -> getDmlBindingValue(column));
        sqlAssembler.appendSqlSlice(")");

        return sqlAssembler.confirm();
    }

    private String getDmlBindingValue(Column<?> column) {
        Type type = this.databaseDialect.getType(column);
        if (type == null) {
            return this.databaseDialect.getQueryBindingWithValueCast(column);
        }
        return type.getQueryBindingWithValue(this.databaseDialect, column);
    }

    private String assembleCreateDatabaseSql(CatalogChanges catalogChanges) {
        SqlStatementAssembler assembler = new SqlStatementAssembler();
        assembler.appendSqlSliceLists("CREATE DATABASE IF NOT EXISTS ");
        assembler.appendSqlSlice(
            ((AbstractGeneralDatabaseDialect<?, ?>) databaseDialect).getQualifiedText(catalogChanges.getCatalog().getName()));
        return assembler.confirm();
    }

    private String assembleDropDatabaseSql(CatalogChanges catalogChanges) {
        SqlStatementAssembler assembler = new SqlStatementAssembler();
        assembler.appendSqlSliceLists("DROP DATABASE IF EXISTS ");
        assembler.appendSqlSlice(
            ((AbstractGeneralDatabaseDialect<?, ?>) databaseDialect).getQualifiedText(catalogChanges.getCatalog().getName()));
        return assembler.confirm();
    }

    private String assembleAlertDatabaseSql(CatalogChanges catalogChanges) {
        SqlStatementAssembler assembler = new SqlStatementAssembler();
        //todo
        return assembler.confirm();
    }

    private String assembleCreateTableSql(CatalogChanges catalogChanges) {
        SqlStatementAssembler assembler = new SqlStatementAssembler();
        assembler.appendSqlSlice("CREATE TABLE IF NOT EXISTS ");
        Table table = catalogChanges.getTable();
        assembler.appendSqlSlice(((AbstractGeneralDatabaseDialect<?, ?>) databaseDialect).getQualifiedTableName(table.getTableId()));
        assembler.appendSqlSlice(" (");
        // assemble columns
        List<? extends Column> columns = catalogChanges.getColumns().stream().sorted(Comparator.comparingInt(Column::getOrder))
            .collect(Collectors.toList());
        List<String> columnNames = columns.stream().map(item -> item.getName()).collect(Collectors.toList());
        Map<String, Column> columnMap = columns.stream().collect(Collectors.toMap(Column::getName, item -> item));
        assembler.appendSqlSliceLists(", ", columnNames, (columnName) -> {
            StringBuilder builder = new StringBuilder();
            //assemble column name
            builder.append(((AbstractGeneralDatabaseDialect<?, ?>) databaseDialect).getQualifiedText(columnName));
            //assemble column type
            Column column = columnMap.get(columnName);
            String typeName = this.databaseDialect.getTypeName(hibernateDialect,column);
            builder.append(" ").append(typeName);

            builder.append(" ").append(this.databaseDialect.getCharsetOrCollateFormatted(column));
            if (Optional.ofNullable(table.getPrimaryKey().getColumnNames()).orElse(new ArrayList<>(0)).contains(columnName)) {
                builder.append(" NOT NULL ");
                if (column.isAutoIncremented()) {
                    builder.append(this.databaseDialect.getAutoIncrementFormatted(column));
                }
            } else {
                if (column.isNotNull()) {
                    builder.append(" NOT NULL ");
                }
            }
            builder.append(" ").append(this.databaseDialect.getDefaultValueFormatted(column));
            builder.append(" ").append(this.databaseDialect.getCommentFormatted(column));
            //assemble column default value
            return builder.toString();
        });
        //assemble primary key and others key
        assembler.appendSqlSlice(", PRIMARY KEY(");
        assembler.appendSqlSliceLists(",", catalogChanges.getTable().getPrimaryKey().getColumnNames(),
            (columnName) -> ((AbstractGeneralDatabaseDialect<?, ?>) databaseDialect).getQualifiedText(columnName));
        assembler.appendSqlSlice(")");
        assembler.appendSqlSlice(")");
        assembler.appendSqlSlice(this.databaseDialect.getTableOptionsFormatted(catalogChanges.getTable()));
        return assembler.confirm();
    }

    private String assembleDropTableSql(CatalogChanges catalogChanges) {
        SqlStatementAssembler assembler = new SqlStatementAssembler();
        assembler.appendSqlSlice("DROP TABLE IF EXISTS ");
        assembler.appendSqlSlice(
            ((AbstractGeneralDatabaseDialect<?, ?>) databaseDialect).getQualifiedTableName(catalogChanges.getTable().getTableId()));
        return assembler.confirm();
    }

    private String assembleAlertTableSql(CatalogChanges catalogChanges) {

        SqlStatementAssembler assembler = new SqlStatementAssembler();

        return assembler.confirm();
    }
}
