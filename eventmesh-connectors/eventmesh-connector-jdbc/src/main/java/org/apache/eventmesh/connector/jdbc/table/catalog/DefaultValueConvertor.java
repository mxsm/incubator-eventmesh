package org.apache.eventmesh.connector.jdbc.table.catalog;

@FunctionalInterface
public interface DefaultValueConvertor {

    /**
     * Parse the default value expression.
     *
     * @param column the column
     * @param defaultValueExpression the default value expression
     * @return the parsed default value
     */
    Object parseDefaultValue(Column<?> column, String defaultValueExpression);

}