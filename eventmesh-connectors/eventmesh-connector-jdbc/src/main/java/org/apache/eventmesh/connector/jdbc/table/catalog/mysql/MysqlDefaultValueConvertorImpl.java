package org.apache.eventmesh.connector.jdbc.table.catalog.mysql;


import org.apache.eventmesh.connector.jdbc.table.catalog.Column;
import org.apache.eventmesh.connector.jdbc.table.catalog.DefaultValueConvertor;

import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.JDBCType;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

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

    private Object convert2Boolean(Column<?> column, String value) {

        // value maybe is numeric or string
        if (StringUtils.isNumeric(value)) {
            return Integer.parseInt(value)!= 0;
        }

        return Boolean.parseBoolean(value);
    }

    private Object convert2Decimal(Column<?> column, String value) {
        return Optional.ofNullable(column.getDecimal()).isPresent() ? new BigDecimal(value).setScale(column.getDecimal(), RoundingMode.HALF_UP) : new BigDecimal(value);
    }

    private Object convertToBits(Column<?> column, String value) {
        //value: '101010111'
        if(column.getColumnLength() > 1){
            int nums = value.length() / Byte.SIZE + (value.length() % Byte.SIZE == 0 ? 0 : 1);
            byte[] bytes = new byte[nums];
            int length = value.length();
            for (int i = 0; i < nums; i++) {
                int size = value.length() - Byte.SIZE < 0 ? 0 : value.length() - Byte.SIZE;
                bytes[nums - i - 1] = (byte) Integer.parseInt(value.substring(size, length), 2);
                value = value.substring(0, size);
            }
            return bytes;
        }

        //value: '1' or '0' parse to boolean
        return Short.parseShort(value) != 0;
    }

    private Object convertToDuration(Column<?> column, String value) {

        return null;
    }

    private Object convertToTimestamp(Column<?> column, String value) {
        return null;
    }

    private Object convertToLocalDateTime(Column<?> column, String value) {
        if(StringUtils.containsAny(value, "CURRENT_TIMESTAMP","current_timestamp")){
            return value;
        }
        //The TIMESTAMP data type is used for values that contain both date and time parts.
        // TIMESTAMP has a range of '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC.
        return LocalDateTime.from(timestampFormat(Optional.ofNullable(column.getColumnLength()).orElse(0L).intValue()).parse(value));
    }

    private Object convert2LocalDate(Column<?> column, String value) {
        //The DATE type is used for values with a date part but no time part.
        // MySQL retrieves and displays DATE values in 'YYYY-MM-DD' format.
        // The supported range is '1000-01-01' to '9999-12-31'.

        return LocalDate.parse(value);
    }


    private DateTimeFormatter timestampFormat(int length) {
        final DateTimeFormatterBuilder dtf = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .optionalStart()
            .appendLiteral(" ")
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .optionalEnd()
            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0);
        if (length > 0) {
            dtf.appendFraction(ChronoField.MICRO_OF_SECOND, 0, length, true);
        }
        return dtf.toFormatter();
    }

}
