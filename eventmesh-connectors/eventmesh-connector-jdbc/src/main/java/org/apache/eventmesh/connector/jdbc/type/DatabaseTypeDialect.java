package org.apache.eventmesh.connector.jdbc.type;

import org.apache.eventmesh.connector.jdbc.table.catalog.Column;
import org.apache.eventmesh.connector.jdbc.table.catalog.Table;

public interface DatabaseTypeDialect {

    String EMPTY_STRING = "";

    Type getType(Column<?> column);

    default String getBooleanFormatted(boolean value) {
        return value ? Boolean.TRUE.toString() : Boolean.FALSE.toString();
    }

    default String getAutoIncrementFormatted(Column<?> column) {
        return EMPTY_STRING;
    }

    default String getDefaultValueFormatted(Column<?> column) {
        return EMPTY_STRING;
    }

    default String getChartsetOrCollateFormatted(Column<?> column) {
        return EMPTY_STRING;
    }

    default String getTableOptionsFormatted(Table table) {
        return EMPTY_STRING;
    }

    default String getQueryBindingWithValueCast(Column<?> column){
        return "?";
    }
}
