package org.apache.eventmesh.connector.jdbc.type;

import org.apache.eventmesh.connector.jdbc.table.catalog.Column;

public interface DatabaseTypeDialect {

    Type getType(Column<?> column);

    default String getBoolenFormatted(boolean value){
        return Boolean.valueOf(value).toString();
    }

}
