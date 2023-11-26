package org.apache.eventmesh.connector.jdbc.table.catalog;

import org.apache.eventmesh.connector.jdbc.table.type.EventMeshDataType;
import org.apache.eventmesh.connector.jdbc.table.type.EventMeshTypeNameConverter;
import org.apache.eventmesh.connector.jdbc.type.eventmesh.NullEventMeshDataType;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

public class EventMeshDataTypeJsonDeserializer extends JsonDeserializer<EventMeshDataType> {

    @Override
    public EventMeshDataType deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JacksonException {
        JsonNode treeNode = jsonParser.readValueAsTree();
        Iterator<Entry<String, JsonNode>> fields = treeNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            if (StringUtils.equals("dataType", field.getKey())) {
                String value = field.getValue().asText();
                return EventMeshTypeNameConverter.ofEventMeshDataType(value);
            }
        }
        return NullEventMeshDataType.INSTANCE;
    }
}