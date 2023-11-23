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

package org.apache.eventmesh.connector.jdbc.dialect;

import org.apache.eventmesh.connector.jdbc.config.JdbcConfig;
import org.apache.eventmesh.connector.jdbc.connection.JdbcConnection;
import org.apache.eventmesh.connector.jdbc.exception.JdbcConnectionException;
import org.apache.eventmesh.connector.jdbc.table.catalog.Column;
import org.apache.eventmesh.connector.jdbc.table.catalog.TableId;
import org.apache.eventmesh.connector.jdbc.table.type.EventMeshDataType;
import org.apache.eventmesh.connector.jdbc.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractGeneralDatabaseDialect<JC extends JdbcConnection, Col extends Column> implements DatabaseDialect<JC> {

    private static final int DEFAULT_BATCH_MAX_ROWS = 20;

    private JdbcConfig config;

    private int batchMaxRows = DEFAULT_BATCH_MAX_ROWS;

    private final Map<String, Type> typeRegisters = new HashMap<>(32);

    public AbstractGeneralDatabaseDialect(JdbcConfig config) {
        this.config = config;
    }

    @Override
    public boolean isValid(Connection connection, int timeout) throws JdbcConnectionException, SQLException {
        return connection == null ? false : connection.isValid(timeout);
    }

    @Override
    public PreparedStatement createPreparedStatement(Connection connection, String sql) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        if (batchMaxRows > 0) {
            preparedStatement.setFetchSize(batchMaxRows);
        }
        return preparedStatement;
    }

    @Override
    public Type getType(Column<?> column) {
        return Optional.ofNullable(typeRegisters.get(column.getNativeType())).orElseGet(()->typeRegisters.get(column.getJdbcType().getName()));
    }

    protected void registerTypes() {
        registerType(EventMeshDataType.BOOLEAN_TYPE);
        registerType(EventMeshDataType.BOOLEAN_ARRAY_TYPE);
        registerType(EventMeshDataType.BYTE_ARRAY_TYPE);
        registerType(EventMeshDataType.BYTE_TYPE);
        registerType(EventMeshDataType.BYTES_TYPE);
        registerType(EventMeshDataType.DOUBLE_ARRAY_TYPE);
        registerType(EventMeshDataType.DOUBLE_TYPE);
        registerType(EventMeshDataType.FLOAT_ARRAY_TYPE);
        registerType(EventMeshDataType.FLOAT_TYPE);
        registerType(EventMeshDataType.INT_ARRAY_TYPE);
        registerType(EventMeshDataType.INT_TYPE);
        registerType(EventMeshDataType.LONG_ARRAY_TYPE);
        registerType(EventMeshDataType.LONG_TYPE);
        registerType(EventMeshDataType.LOCAL_DATE_TIME_TYPE);
        registerType(EventMeshDataType.LOCAL_DATE_TYPE);
        registerType(EventMeshDataType.LOCAL_TIME_TYPE);
        registerType(EventMeshDataType.SHORT_ARRAY_TYPE);
        registerType(EventMeshDataType.SHORT_TYPE);
        registerType(EventMeshDataType.STRING_ARRAY_TYPE);
        registerType(EventMeshDataType.STRING_TYPE);
        registerType(EventMeshDataType.VOID_TYPE);
    }

    protected void registerType(Type type) {
        Optional.ofNullable(type.ofRegistrationKeys()).orElse(new ArrayList<>(0)).forEach(key -> {
            typeRegisters.put(key, type);
        });
    }

    public abstract String getQualifiedTableName(TableId tableId);

    public abstract String getQualifiedText(String text);
}
