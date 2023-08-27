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

package org.apache.eventmesh.connector.jdbc.source.dialect.antlr4.mysql.listener;

import org.apache.eventmesh.connector.jdbc.antlr4.autogeneration.MySqlParser.ColumnCreateTableContext;
import org.apache.eventmesh.connector.jdbc.antlr4.autogeneration.MySqlParser.CopyCreateTableContext;
import org.apache.eventmesh.connector.jdbc.antlr4.autogeneration.MySqlParser.QueryCreateTableContext;
import org.apache.eventmesh.connector.jdbc.antlr4.autogeneration.MySqlParser.TableNameContext;
import org.apache.eventmesh.connector.jdbc.source.dialect.antlr4.mysql.MysqlAntlr4DdlParser;
import org.apache.eventmesh.connector.jdbc.table.catalog.CatalogTableSet;
import org.apache.eventmesh.connector.jdbc.table.catalog.TableEditor;
import org.apache.eventmesh.connector.jdbc.table.catalog.TableId;
import org.apache.eventmesh.connector.jdbc.table.catalog.TableSchema;
import org.apache.eventmesh.connector.jdbc.utils.Antlr4Utils;

import java.util.List;

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * <pre>
 * listen for events while parsing a CREATE TABLE statement in a MySQL DDL (Data Definition Language) script.
 * </pre>
 *
 * <a href="https://dev.mysql.com/doc/refman/8.0/en/create-table.html">MYSQL CREATE TABLE</a>
 *
 * <pre>
 * CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
 *     (create_definition,...)
 *     [table_options]
 *     [partition_options]
 * </pre>
 *
 * <pre>
 * CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
 *     [(create_definition,...)]
 *     [table_options]
 *     [partition_options]
 *     [IGNORE | REPLACE]
 *     [AS] query_expression
 * </pre>
 * <pre>
 * CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
 *     { LIKE old_tbl_name | (LIKE old_tbl_name) }
 * </pre>
 */
public class CreateTableParserListener extends TableBaseParserListener {

    public CreateTableParserListener(List<ParseTreeListener> listeners, MysqlAntlr4DdlParser parser) {
        super(listeners, parser);
    }

    @Override
    public void enterCopyCreateTable(CopyCreateTableContext ctx) {
        List<TableNameContext> tableNameContexts = ctx.tableName();
        String targetTableName = tableNameContexts.get(0).fullId().getText();
        String sourceTableName = null;
        if (tableNameContexts.size() == 2) {
            sourceTableName = tableNameContexts.get(1).fullId().getText();
        } else {
            sourceTableName = ctx.parenthesisTable.fullId().getText();
        }
        this.tableEditor = createTableEditor(targetTableName);
        this.tableEditor.withSql(Antlr4Utils.getText(ctx));
        super.enterCopyCreateTable(ctx);
    }

    @Override
    public void enterQueryCreateTable(QueryCreateTableContext ctx) {
        String tableName = ctx.tableName().fullId().getText();
        this.tableEditor = createTableEditor(tableName);
        this.tableEditor.withSql(Antlr4Utils.getText(ctx));
        super.enterQueryCreateTable(ctx);
    }

    @Override
    public void enterColumnCreateTable(ColumnCreateTableContext ctx) {
        String tableName = ctx.tableName().fullId().getText();
        this.tableEditor = createTableEditor(tableName);
        this.tableEditor.withSql(Antlr4Utils.getText(ctx));
        super.enterColumnCreateTable(ctx);
    }

    @Override
    public void exitColumnCreateTable(ColumnCreateTableContext ctx) {

        parser.runIfAllNotNull(() -> {
            listeners.remove(columnDefinitionListener);
            //help JVM GC
            columnDefinitionListener = null;
            TableSchema tableSchema = tableEditor.create();
            parser.handleEvent(null);
        }, tableEditor);

        super.exitColumnCreateTable(ctx);
    }

    private TableEditor createTableEditor(String tableName) {
        TableId tableId = parser.parseTableId(tableName);
        return CatalogTableSet.ofCatalogTableEditor(tableId);
    }
}
