package org.apache.eventmesh.connector.jdbc.type.mysql;

import org.apache.eventmesh.connector.jdbc.dialect.DatabaseDialect;
import org.apache.eventmesh.connector.jdbc.table.catalog.Column;
import org.apache.eventmesh.connector.jdbc.type.AbstractType;

import org.apache.commons.collections4.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EnumType extends AbstractType {

    public static final EnumType INSTANCE = new EnumType();

    @Override
    public List<String> ofRegistrationKeys() {
        return Arrays.asList("SET");
    }

    @Override
    public String getTypeName(DatabaseDialect<?> databaseDialect, Column<?> column) {
        //https://dev.mysql.com/doc/refman/8.0/en/enum.html
        List<String> enumValues = column.getEnumValues();
        if (CollectionUtils.isNotEmpty(enumValues)) {
            return "ENUM(" + enumValues.stream().map(val -> "'" + val + "'").collect(Collectors.joining(", ")) + ")";
        }
        return "ENUM()";
    }
}