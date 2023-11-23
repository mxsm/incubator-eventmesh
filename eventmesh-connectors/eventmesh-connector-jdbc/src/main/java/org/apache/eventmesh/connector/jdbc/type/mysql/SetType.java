package org.apache.eventmesh.connector.jdbc.type.mysql;

import org.apache.eventmesh.connector.jdbc.dialect.DatabaseDialect;
import org.apache.eventmesh.connector.jdbc.table.catalog.Column;
import org.apache.eventmesh.connector.jdbc.type.AbstractType;

import org.apache.commons.collections4.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class SetType extends AbstractType {

    public static final SetType INSTANCE = new SetType();

    @Override
    public List<String> ofRegistrationKeys() {
        return Arrays.asList("SET","set");
    }

    @Override
    public String getTypeName(DatabaseDialect<?> databaseDialect, Column<?> column) {
        //https://dev.mysql.com/doc/refman/8.0/en/set.html
        List<String> enumValues = column.getEnumValues();
        if (CollectionUtils.isNotEmpty(enumValues)) {
            return "SET(" + enumValues.stream().map(val -> "'" + val + "'").collect(Collectors.joining(", ")) + ")";
        }
        return "SET()";
    }
}