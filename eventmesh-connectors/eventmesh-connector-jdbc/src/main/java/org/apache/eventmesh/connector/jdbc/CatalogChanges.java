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

package org.apache.eventmesh.connector.jdbc;

import org.apache.eventmesh.connector.jdbc.event.SchemaChangeEventType;
import org.apache.eventmesh.connector.jdbc.table.catalog.CatalogSchema;
import org.apache.eventmesh.connector.jdbc.table.catalog.Column;
import org.apache.eventmesh.connector.jdbc.table.catalog.Table;

import java.util.List;

import lombok.Data;

@Data
public class CatalogChanges {

    private String type;

    private String operationType;

    private CatalogSchema catalog;

    private Table table;

    private List<? extends Column> columns;

    private CatalogChanges(String type, String operationType, CatalogSchema catalog, Table table,
        List<? extends Column> columns) {
        this.type = type;
        this.operationType = operationType;
        this.catalog = catalog;
        this.table = table;
        this.columns = columns;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String type;
        private String operationType;
        private CatalogSchema catalog;
        private Table table;
        private List<? extends Column> columns;


        public Builder operationType(SchemaChangeEventType changeEventType) {
            this.type = changeEventType.ofType();
            this.operationType = changeEventType.ofOperationType();
            return this;
        }

        public Builder catalog(CatalogSchema catalog) {
            this.catalog = catalog;
            return this;
        }

        public Builder table(Table table) {
            this.table = table;
            return this;
        }

        public Builder columns(List<? extends Column> columns) {
            this.columns = columns;
            return this;
        }

        public CatalogChanges build() {
            return new CatalogChanges(type, operationType, catalog, table, columns);
        }
    }


}
