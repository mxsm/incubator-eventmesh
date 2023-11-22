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

package org.apache.eventmesh.connector.jdbc.type.mysql;

import org.apache.eventmesh.connector.jdbc.dialect.DatabaseDialect;
import org.apache.eventmesh.connector.jdbc.table.catalog.Column;
import org.apache.eventmesh.connector.jdbc.type.AbstractType;

import java.util.Arrays;
import java.util.List;

public class JsonType extends AbstractType {

    public static final JsonType INSTANCE = new JsonType();

    /**
     * @return
     */
    @Override
    public List<String> ofRegistrationKeys() {
        return Arrays.asList("json", "JSON", "Json");
    }

    /**
     * @param databaseDialect
     * @param column
     * @return
     */
    @Override
    public String getTypeName(DatabaseDialect<?> databaseDialect, Column<?> column) {
        return "json";
    }

    /**
     * @param databaseDialect
     * @param column
     * @return
     */
    @Override
    public String getQueryBindingWithValue(DatabaseDialect<?> databaseDialect, Column<?> column) {
        return "CAST(? AS JSON)";
    }
}
