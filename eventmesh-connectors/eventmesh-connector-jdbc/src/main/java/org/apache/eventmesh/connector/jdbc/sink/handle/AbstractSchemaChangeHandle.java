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

import org.apache.eventmesh.connector.jdbc.CatalogChanges;
import org.apache.eventmesh.connector.jdbc.JdbcConnectData;
import org.apache.eventmesh.connector.jdbc.Payload;
import org.apache.eventmesh.connector.jdbc.dialect.DatabaseDialect;
import org.apache.eventmesh.connector.jdbc.dialect.DatabaseType;
import org.apache.eventmesh.connector.jdbc.dialect.SqlStatementAssembler;
import org.apache.eventmesh.connector.jdbc.event.SchemaChangeEventType;
import org.apache.eventmesh.connector.jdbc.source.SourceMateData;
import org.apache.eventmesh.connector.jdbc.table.catalog.TableId;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSchemaChangeHandle implements SchemaChangeHandle {

    private DatabaseDialect dialect;

    /**
     * Handles a schema change using the specified JDBC connection data.
     *
     * @param connectData the JDBC connection data
     */
    @Override
    public void handle(JdbcConnectData connectData) throws Exception {

    }



}
