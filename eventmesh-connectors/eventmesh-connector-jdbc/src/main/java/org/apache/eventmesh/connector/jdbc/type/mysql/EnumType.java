package org.apache.eventmesh.connector.jdbc.type.mysql;

import java.util.Arrays;
import java.util.List;
import org.apache.eventmesh.connector.jdbc.dialect.DatabaseDialect;
import org.apache.eventmesh.connector.jdbc.table.catalog.Column;
import org.apache.eventmesh.connector.jdbc.type.AbstractType;


public class EnumType extends AbstractType {

    public static final EnumType INSTANCE = new EnumType();

    @Override
    public List<String> ofRegistrationKeys() {
        return Arrays.asList("SET");
    }

    @Override
    public String getTypeName(DatabaseDialect<?> databaseDialect, Column<?> column) {
        //https://dev.mysql.com/doc/refman/8.0/en/enum.html
        return null;
    }
}