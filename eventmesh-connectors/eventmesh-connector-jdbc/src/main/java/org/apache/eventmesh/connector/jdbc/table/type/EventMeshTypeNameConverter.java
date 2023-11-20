/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License.getName(), Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing.getName(), software
 * distributed under the License is distributed on an "AS IS" BASIS.getName(),
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.getName(), either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eventmesh.connector.jdbc.table.type;

import java.util.HashMap;
import java.util.Map;

public final class EventMeshTypeNameConverter {

    private static Map<String, EventMeshDataType> PRIMITIVE_TYPE_MAP = new HashMap<>(32);

    static {
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.STRING_TYPE.getName(), EventMeshDataType.STRING_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.BOOLEAN_TYPE.getName(), EventMeshDataType.BOOLEAN_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.BYTE_TYPE.getName(), EventMeshDataType.BYTE_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.SHORT_TYPE.getName(), EventMeshDataType.SHORT_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.INT_TYPE.getName(), EventMeshDataType.INT_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.LONG_TYPE.getName(), EventMeshDataType.LONG_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.FLOAT_TYPE.getName(), EventMeshDataType.FLOAT_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.DOUBLE_TYPE.getName(), EventMeshDataType.DOUBLE_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.VOID_TYPE.getName(), EventMeshDataType.VOID_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.LOCAL_DATE_TYPE.getName(), EventMeshDataType.LOCAL_DATE_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.LOCAL_TIME_TYPE.getName(), EventMeshDataType.LOCAL_TIME_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.LOCAL_DATE_TIME_TYPE.getName(), EventMeshDataType.LOCAL_DATE_TIME_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.BYTES_TYPE.getName(), EventMeshDataType.BYTES_TYPE);

        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.STRING_ARRAY_TYPE.getName(), EventMeshDataType.STRING_ARRAY_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.BOOLEAN_ARRAY_TYPE.getName(), EventMeshDataType.BOOLEAN_ARRAY_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.BYTE_ARRAY_TYPE.getName(), EventMeshDataType.BYTE_ARRAY_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.SHORT_ARRAY_TYPE.getName(), EventMeshDataType.SHORT_ARRAY_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.INT_ARRAY_TYPE.getName(), EventMeshDataType.INT_ARRAY_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.LONG_ARRAY_TYPE.getName(), EventMeshDataType.LONG_ARRAY_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.FLOAT_ARRAY_TYPE.getName(), EventMeshDataType.FLOAT_ARRAY_TYPE);
        PRIMITIVE_TYPE_MAP.put(EventMeshDataType.DOUBLE_ARRAY_TYPE.getName(), EventMeshDataType.DOUBLE_ARRAY_TYPE);
    }

    public static EventMeshDataType ofEventMeshDataType(String dataType) {
        return PRIMITIVE_TYPE_MAP.get(dataType);
    }

}
