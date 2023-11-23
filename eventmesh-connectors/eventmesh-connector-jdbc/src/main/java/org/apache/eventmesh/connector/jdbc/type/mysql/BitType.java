package org.apache.eventmesh.connector.jdbc.type.mysql;

import org.apache.eventmesh.connector.jdbc.dialect.DatabaseDialect;
import org.apache.eventmesh.connector.jdbc.table.catalog.Column;
import org.apache.eventmesh.connector.jdbc.type.AbstractType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;


public class BitType extends AbstractType {

    public static final BitType INSTANCE = new BitType();

    @Override
    public String getDefaultValue(DatabaseDialect<?> databaseDialect, Column<?> column) {
        return column.getDefaultValue() == null? " NULL " : String.format("b'%s'",column.getDefaultValue());
    }

    @Override
    public String getTypeName(DatabaseDialect<?> databaseDialect, Column<?> column) {
        //https://dev.mysql.com/doc/refman/8.0/en/bit-type.html
        Long columnLength = column.getColumnLength();
        return String.format("bit(%d)", Optional.ofNullable(columnLength).orElse(1L).intValue());
    }

    @Override
    public List<String> ofRegistrationKeys() {
        return Arrays.asList("BIT");
    }
}
