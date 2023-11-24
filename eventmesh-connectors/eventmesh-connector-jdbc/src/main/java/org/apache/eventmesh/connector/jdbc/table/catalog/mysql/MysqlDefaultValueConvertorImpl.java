package org.apache.eventmesh.connector.jdbc.table.catalog.mysql;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.eventmesh.connector.jdbc.table.catalog.Column;
import org.apache.eventmesh.connector.jdbc.table.catalog.DefaultValueConvertor;

public class MysqlDefaultValueConvertorImpl implements DefaultValueConvertor {

    private static final Set<JDBCType> NUMBER_DATA_TYPES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(JDBCType.TINYINT, JDBCType.INTEGER,
        JDBCType.DATE, JDBCType.TIMESTAMP, JDBCType.TIMESTAMP_WITH_TIMEZONE, JDBCType.TIME, JDBCType.BOOLEAN, JDBCType.BIT,
        JDBCType.NUMERIC, JDBCType.DECIMAL, JDBCType.FLOAT, JDBCType.DOUBLE, JDBCType.REAL)));


    @Override
    public Object parseDefaultValue(Column<?> column, String defaultValueExpression) {
        if (null == defaultValueExpression) {
            return null;
        }
        defaultValueExpression = defaultValueExpression.trim();

        if (NUMBER_DATA_TYPES.contains(column.getJdbcType()) && StringUtils.equalsAnyIgnoreCase(defaultValueExpression, Boolean.TRUE.toString(),
            Boolean.FALSE.toString())) {
            /*
             * These types are synonyms for DECIMAL:
             * DEC[(M[,D])] [UNSIGNED] [ZEROFILL],
             * NUMERIC[(M[,D])] [UNSIGNED] [ZEROFILL],
             * FIXED[(M[,D])] [UNSIGNED] [ZEROFILL]
             */
            if(column.getJdbcType() == JDBCType.DECIMAL || column.getJdbcType() == JDBCType.NUMERIC ){
                return convert2Decimal(column,defaultValueExpression);
            }
            return StringUtils.equalsIgnoreCase(Boolean.TRUE.toString(), defaultValueExpression)?1:0;
        }

        switch (column.getJdbcType()) {
            case DATE:
                return convert2LocalDate(column, defaultValueExpression);
            case TIMESTAMP:
                return convertToLocalDateTime(column, defaultValueExpression);
            case TIMESTAMP_WITH_TIMEZONE:
                return convertToTimestamp(column, defaultValueExpression);
            case TIME:
                return convertToDuration(column, defaultValueExpression);
            case BOOLEAN:
                return convert2Boolean(column,defaultValueExpression);
            case BIT:
                return convertToBits(column, defaultValueExpression);

            case NUMERIC:
            case DECIMAL:
                return convert2Decimal(column, defaultValueExpression);

            case FLOAT:
            case DOUBLE:
            case REAL:
                return Double.parseDouble(defaultValueExpression);
        }
        return defaultValueExpression;
    }

    private Object convert2Boolean(Column column, String value) {

        // value maybe is numeric or string
        if (StringUtils.isNumeric(value)) {
            return Integer.parseInt(value)!= 0;
        }

        return Boolean.parseBoolean(value);
    }

    private Object convert2Decimal(Column column, String value) {
        return Optional.ofNullable(column.getDecimal()).isPresent() ? new BigDecimal(value).setScale(column.getDecimal(), RoundingMode.HALF_UP) : new BigDecimal(value);
    }

    private Object convertToBits(Column column, String value) {


        return null;
    }

    private Object convertToDuration(Column column, String value) {

        return null;
    }

    private Object convertToTimestamp(Column column, String value) {
        return null;
    }

    private Object convertToLocalDateTime(Column column, String value) {
        return null;
    }

    private Object convert2LocalDate(Column column, String value) {
        return null;
    }


}
