package org.apache.eventmesh.connector.jdbc.type.mysql;

import java.util.Arrays;
import java.util.List;
import org.apache.eventmesh.connector.jdbc.dialect.DatabaseDialect;
import org.apache.eventmesh.connector.jdbc.table.catalog.Column;
import org.apache.eventmesh.connector.jdbc.type.AbstractType;


public class SetType extends AbstractType {

    public static final SetType INSTANCE = new SetType();

    @Override
    public List<String> ofRegistrationKeys() {
        return Arrays.asList("SET");
    }

    @Override
    public String getTypeName(DatabaseDialect<?> databaseDialect, Column<?> column) {
        //https://dev.mysql.com/doc/refman/8.0/en/set.html
        return null;
    }
}