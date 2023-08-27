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

import org.apache.eventmesh.connector.jdbc.antlr4.autogeneration.MySqlParser.DropViewContext;
import org.apache.eventmesh.connector.jdbc.antlr4.autogeneration.MySqlParserBaseListener;
import org.apache.eventmesh.connector.jdbc.source.dialect.antlr4.mysql.MysqlAntlr4DdlParser;
import org.apache.eventmesh.connector.jdbc.utils.JdbcStringUtils;

import org.antlr.v4.runtime.misc.Interval;

/**
 * A custom ANTLR listener for parsing DROP TABLE statements.
 * <a href="https://dev.mysql.com/doc/refman/8.0/en/drop-view.html">DROP VIEW Statement</a>
 * <pre>
 *  DROP VIEW [IF EXISTS]
 *     view_name [, view_name] ...
 *     [RESTRICT | CASCADE]
 * </pre>
 */
public class DropViewParserListener extends MySqlParserBaseListener {

    private MysqlAntlr4DdlParser parser;

    /**
     * Constructs a DropViewParserListener with the specified parser.
     *
     * @param parser The MysqlAntlr4DdlParser used for parsing.
     */
    public DropViewParserListener(MysqlAntlr4DdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterDropView(DropViewContext ctx) {

        final String sqlPrefix = parseSqlPrefix(ctx);
        ctx.fullId().stream().forEach(fullIdContext -> {
            String viewName = JdbcStringUtils.withoutWrapper(fullIdContext.getText());
            //DropViewEvent event = new DropViewEvent(parser.getCurrentDatabase(), viewName, sqlPrefix + viewName);
            parser.handleEvent(null);
        });
        super.enterDropView(ctx);
    }

    /**
     * Parses the SQL prefix before the DROP TABLE statement.
     *
     * @param ctx The DropTableContext representing the drop table statement.
     * @return The SQL prefix before the DROP TABLE statement.
     */
    private String parseSqlPrefix(DropViewContext ctx) {
        Interval interval = new Interval(ctx.start.getStartIndex(), ctx.fullId().get(0).start.getStartIndex() - 1);
        return ctx.start.getInputStream().getText(interval);
    }
}

