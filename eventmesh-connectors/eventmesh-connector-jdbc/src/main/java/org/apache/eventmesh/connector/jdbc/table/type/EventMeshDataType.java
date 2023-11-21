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

package org.apache.eventmesh.connector.jdbc.table.type;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;

import org.apache.eventmesh.connector.jdbc.dialect.DatabaseDialect;
import org.apache.eventmesh.connector.jdbc.table.catalog.Column;
import org.apache.eventmesh.connector.jdbc.type.Type;

public enum EventMeshDataType implements Type {
    BYTE_TYPE(Byte.class, SQLType.TINYINT, "INT_8"),
    SHORT_TYPE(Short.class, SQLType.SMALLINT, "INT_16"),
    INT_TYPE(Integer.class, SQLType.INTEGER, "INT_32"),
    LONG_TYPE(Long.class, SQLType.BIGINT, "INT_64"),
    FLOAT_TYPE(Float.class, SQLType.FLOAT, "FLOAT_32"),
    DOUBLE_TYPE(Double.class, SQLType.DOUBLE, "FLOAT_64"),
    BOOLEAN_TYPE(Boolean.class, SQLType.BOOLEAN, "BOOLEAN"),
    STRING_TYPE(String.class, SQLType.STRING, "STRING"),
    VOID_TYPE(Void.class, SQLType.NULL, "VOID"),
    BIG_DECIMAL_TYPE(BigDecimal.class, SQLType.DECIMAL, "decimal"),
    BYTES_TYPE(byte[].class, SQLType.BINARY, "bytes"),
    LOCAL_DATE_TYPE(LocalDate.class, SQLType.DATE, "LocalDate"),
    LOCAL_TIME_TYPE(LocalTime.class, SQLType.TIME, "LocalTime"),
    LOCAL_DATE_TIME_TYPE(LocalDateTime.class, SQLType.TIMESTAMP, "LocalDateTime"),

    BYTE_ARRAY_TYPE(byte[].class, SQLType.BINARY, "byte-array"),
    SHORT_ARRAY_TYPE(short[].class, SQLType.SMALLINT, "short-array"),
    INT_ARRAY_TYPE(int[].class, SQLType.INTEGER, "int-array"),
    LONG_ARRAY_TYPE(long[].class, SQLType.BIGINT, "long-array"),
    FLOAT_ARRAY_TYPE(float[].class, SQLType.FLOAT, "float-array"),
    DOUBLE_ARRAY_TYPE(double[].class, SQLType.DOUBLE, "double-array"),
    BOOLEAN_ARRAY_TYPE(boolean[].class, SQLType.BOOLEAN, "boolean-array"),
    STRING_ARRAY_TYPE(String[].class, SQLType.STRING, "string-array");


    private final Class<?> typeClass;

    private final SQLType sqlType;

    private final String name;

    EventMeshDataType(Class<?> typeClass, SQLType sqlType, String name) {
        this.typeClass = typeClass;
        this.sqlType = sqlType;
        this.name = name;
    }

    /**
     * Returns the type class of the data.
     *
     * @return the type class of the data.
     */
    public Class<?> getTypeClass() {
        return typeClass;
    }

    /**
     * Returns the SQL type of the data.
     *
     * @return the SQL type of the data.
     */

    public SQLType getSQLType() {
        return sqlType;
    }

    /**
     * Gets the name of the data type.
     *
     * @return The name of the data type.
     */
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "EventMeshDataType{" +
            "typeClass=" + typeClass +
            ", sqlType=" + sqlType +
            ", name='" + name + '\'' +
            '}';
    }

    @Override
    public List<String> ofRegistrationKeys() {
        return Arrays.asList(this.name);
    }

    @Override
    public String getDefaultValue(DatabaseDialect<?> databaseDialect, Column<?> column) {
        return null;
    }

    @Override
    public String getTypeName(DatabaseDialect<?> databaseDialect, Column<?> column) {
        return this.sqlType.name();
    }
}
