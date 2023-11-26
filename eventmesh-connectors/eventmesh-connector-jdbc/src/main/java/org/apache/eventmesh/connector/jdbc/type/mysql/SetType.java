package org.apache.eventmesh.connector.jdbc.type.mysql;

import org.apache.eventmesh.connector.jdbc.dialect.DatabaseDialect;
import org.apache.eventmesh.connector.jdbc.table.catalog.Column;
import org.apache.eventmesh.connector.jdbc.table.type.SQLType;
import org.apache.eventmesh.connector.jdbc.type.AbstractType;

import org.apache.commons.collections4.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class SetType extends AbstractType<String> {

    public static final SetType INSTANCE = new SetType();

    private SetType() {
        super(String.class, SQLType.STRING, "SET");
    }

    @Override
    public List<String> ofRegistrationKeys() {
        return Arrays.asList(getName(), "set");
    }

    @Override
    public String getTypeName(Column<?> column) {
        //https://dev.mysql.com/doc/refman/8.0/en/set.html
        List<String> enumValues = column.getEnumValues();
        if (CollectionUtils.isNotEmpty(enumValues)) {
            return "SET(" + enumValues.stream().map(val -> "'" + val + "'").collect(Collectors.joining(", ")) + ")";
        }
        return "SET()";
    }

    @Override
    public String getDefaultValue(DatabaseDialect<?> databaseDialect, Column<?> column) {
        if (column.getDefaultValue() == null) {
            return "NULL";
        }
        return "'" + column.getDefaultValue() + "'";
    }
}