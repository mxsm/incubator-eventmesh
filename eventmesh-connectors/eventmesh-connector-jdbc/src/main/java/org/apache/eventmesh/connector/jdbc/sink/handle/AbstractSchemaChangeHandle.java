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

import org.apache.eventmesh.connector.jdbc.JdbcConnectData;
import org.apache.eventmesh.connector.jdbc.Payload;
import org.apache.eventmesh.connector.jdbc.dialect.DatabaseDialect;

import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.connector.jdbc.source.SourceMateData;
import org.hibernate.dialect.Dialect;

@Slf4j
public class AbstractSchemaChangeHandle implements SchemaChangeHandle {

    protected DatabaseDialect eventMeshDialect;

    protected Dialect hibernateDialect;

    protected DialectAssemblyLine dialectAssemblyLine;

    public AbstractSchemaChangeHandle(DatabaseDialect eventMeshDialect, Dialect hibernateDialect) {
        this.eventMeshDialect = eventMeshDialect;
        this.hibernateDialect = hibernateDialect;
        this.dialectAssemblyLine = new GeneralDialectAssemblyLine(eventMeshDialect, hibernateDialect);
    }

    /**
     * Handles a schema change using the specified JDBC connection data.
     *
     * @param connectData the JDBC connection data
     */
    @Override
    public void handle(JdbcConnectData connectData) throws Exception {
        Payload payload = connectData.getPayload();
        SourceMateData sourceMateData = payload.ofSourceMateData();
        String sql = null;
        if (connectData.isSchemaChanges()) {
            //create Database
            sql = this.dialectAssemblyLine.getDatabaseOrTableStatement(sourceMateData, payload.ofCatalogChanges(), payload.ofDdl());
        } else if (connectData.isDataChanges()) {
            //do handle data changes
            sql = this.dialectAssemblyLine.getInsertStatement(sourceMateData, connectData.getSchema(), payload.ofDdl());
        } else {
            //not support changes
        }

        log.info("SQL={}", sql);
    }
}