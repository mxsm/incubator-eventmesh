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

import org.apache.eventmesh.connector.jdbc.antlr4.autogeneration.MySqlParser.DropTableContext;
import org.apache.eventmesh.connector.jdbc.antlr4.autogeneration.MySqlParserBaseListener;
import org.apache.eventmesh.connector.jdbc.source.dialect.antlr4.mysql.MysqlAntlr4DdlParser;
import org.apache.eventmesh.connector.jdbc.utils.JdbcStringUtils;

import org.antlr.v4.runtime.misc.Interval;

/**
 * A custom ANTLR listener for parsing DROP TABLE statements.
 * <a href="https://dev.mysql.com/doc/refman/8.0/en/drop-table.html">DROP TABLE Statement</a>
 * <pre>
 *     DROP [TEMPORARY] TABLE [IF EXISTS]
 *     tbl_name [, tbl_name] ...
 *     [RESTRICT | CASCADE]
 * </pre>
 */
public class DropTableParserListener extends MySqlParserBaseListener {

    private MysqlAntlr4DdlParser parser;

    /**
     * Constructs a DropTableParserListener with the specified parser.
     *
     * @param parser The MysqlAntlr4DdlParser used for parsing.
     */
    public DropTableParserListener(MysqlAntlr4DdlParser parser) {
        this.parser = parser;
    }

    /**
     * Called when entering a DROP TABLE statement in the parse tree.
     *
     * @param ctx The DropTableContext representing the drop table statement.
     */
    @Override
    public void enterDropTable(DropTableContext ctx) {
        String sqlPrefix = parseSqlPrefix(ctx);
        ctx.tables().tableName().stream().forEach(tableNameContext -> {
            String tableName = JdbcStringUtils.withoutWrapper(tableNameContext.fullId().getText());
            if (parser.getCallback() != null) {
                //DropTableEvent event = new DropTableEvent(parser.getCurrentDatabase(), tableName, sqlPrefix + tableName);
                parser.getCallback().handle(null);
            }
        });
        super.enterDropTable(ctx);
    }

    /**
     * Parses the SQL prefix before the DROP TABLE statement.
     *
     * @param ctx The DropTableContext representing the drop table statement.
     * @return The SQL prefix before the DROP TABLE statement.
     */
    private String parseSqlPrefix(DropTableContext ctx) {
        Interval interval = new Interval(ctx.start.getStartIndex(), ctx.tables().start.getStartIndex() - 1);
        return ctx.start.getInputStream().getText(interval);
    }
}

