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

import org.apache.eventmesh.connector.jdbc.antlr4.autogeneration.MySqlParser.CreateIndexContext;
import org.apache.eventmesh.connector.jdbc.antlr4.autogeneration.MySqlParserBaseListener;
import org.apache.eventmesh.connector.jdbc.source.dialect.antlr4.mysql.MysqlAntlr4DdlParser;
import org.apache.eventmesh.connector.jdbc.utils.Antlr4Utils;
import org.apache.eventmesh.connector.jdbc.utils.JdbcStringUtils;

/**
 * <a href="https://dev.mysql.com/doc/refman/8.0/en/create-index.html">CREATE INDEX</a>
 */
public class CreateIndexParserListener extends MySqlParserBaseListener {

    private MysqlAntlr4DdlParser parser;

    public CreateIndexParserListener(MysqlAntlr4DdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterCreateIndex(CreateIndexContext ctx) {

        //create unique index
        if (ctx.UNIQUE() != null) {
            createUniqueIndex(ctx);
        } else if (ctx.FULLTEXT() != null) {
            //
        } else if (ctx.SPATIAL() != null) {
            //
        } else {
            //normal
        }
        super.enterCreateIndex(ctx);
    }

    private void createUniqueIndex(CreateIndexContext ctx) {
        String tableName = JdbcStringUtils.withoutWrapper(ctx.tableName().fullId().getText());
        String sql = Antlr4Utils.getText(ctx);
        //CreateIndexEvent event = new CreateIndexEvent(parser.getCurrentDatabase(), tableName, sql);
        parser.handleEvent(null);
    }

}
