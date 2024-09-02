package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.tibero;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(TypeConverter.class)
public class TiberoTypeConverter implements TypeConverter<BasicTypeDefine> {

    // ============================data types=====================
    // -------------------------number----------------------------
    public static final String TIBERO_BINARY_DOUBLE = "BINARY_DOUBLE";
    public static final String TIBERO_BINARY_FLOAT = "BINARY_FLOAT";
    public static final String TIBERO_NUMBER = "NUMBER";
    public static final String TIBERO_FLOAT = "FLOAT";
    public static final String TIBERO_REAL = "REAL";
    public static final String TIBERO_INTEGER = "INTEGER";

    // -------------------------string----------------------------
    public static final String TIBERO_CHAR = "CHAR";
    public static final String TIBERO_NCHAR = "NCHAR";
    public static final String TIBERO_VARCHAR = "VARCHAR";
    public static final String TIBERO_VARCHAR2 = "VARCHAR2";
    public static final String TIBERO_NVARCHAR2 = "NVARCHAR2";
    public static final String TIBERO_LONG = "LONG";
    public static final String TIBERO_ROWID = "ROWID";
    public static final String TIBERO_CLOB = "CLOB";
    public static final String TIBERO_NCLOB = "NCLOB";
    public static final String TIBERO_XML = "XMLTYPE";
    public static final String TIBERO_SYS_XML = "SYS.XMLTYPE";

    // ------------------------------time-------------------------
    public static final String TIBERO_DATE = "DATE";
    public static final String TIBERO_TIMESTAMP = "TIMESTAMP";
    public static final String TIBERO_TIMESTAMP_WITH_TIME_ZONE =
            TIBERO_TIMESTAMP + " WITH TIME ZONE";
    public static final String TIBERO_TIMESTAMP_WITH_LOCAL_TIME_ZONE =
            TIBERO_TIMESTAMP + " WITH LOCAL TIME ZONE";

    // ------------------------------blob-------------------------
    public static final String TIBERO_BLOB = "BLOB";
    public static final String TIBERO_RAW = "RAW";
    public static final String TIBERO_LONG_RAW = "LONG RAW";

    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_PRECISION = MAX_PRECISION;
    public static final int MAX_SCALE = 127;
    public static final int DEFAULT_SCALE = 18;
    public static final int TIMESTAMP_DEFAULT_SCALE = 6;
    public static final int MAX_TIMESTAMP_SCALE = 9;
    public static final long MAX_RAW_LENGTH = 2000;
    public static final long MAX_ROWID_LENGTH = 18;
    public static final long MAX_CHAR_LENGTH = 2000;
    public static final long MAX_VARCHAR_LENGTH = 4000;

    public static final long BYTES_2GB = (long) Math.pow(2, 31);
    public static final long BYTES_4GB = (long) Math.pow(2, 32);
    public static final TiberoTypeConverter INSTANCE = new TiberoTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.TIBERO;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        String tiberoType = typeDefine.getDataType().toUpperCase();
        switch (tiberoType) {
            case TIBERO_INTEGER:
                builder.dataType(new DecimalType(DEFAULT_PRECISION, 0));
                builder.columnLength((long) DEFAULT_PRECISION);
                break;
            case TIBERO_NUMBER:
                Long precision = typeDefine.getPrecision();
                if (precision == null || precision == 0 || precision > DEFAULT_PRECISION) {
                    precision = Long.valueOf(DEFAULT_PRECISION);
                }
                Integer scale = typeDefine.getScale();
                if (scale == null) {
                    scale = 127;
                }

                if (scale <= 0) {
                    int newPrecision = (int) (precision - scale);
                    if (newPrecision <= 18) {
                        if (newPrecision == 1) {
                            builder.dataType(BasicType.BOOLEAN_TYPE);
                        } else if (newPrecision <= 9) {
                            builder.dataType(BasicType.INT_TYPE);
                        } else {
                            builder.dataType(BasicType.LONG_TYPE);
                        }
                    } else if (newPrecision < 38) {
                        builder.dataType(new DecimalType(newPrecision, 0));
                        builder.columnLength((long) newPrecision);
                    } else {
                        builder.dataType(new DecimalType(DEFAULT_PRECISION, 0));
                        builder.columnLength((long) DEFAULT_PRECISION);
                    }
                } else if (scale <= DEFAULT_SCALE) {
                    builder.dataType(new DecimalType(precision.intValue(), scale));
                    builder.columnLength(precision);
                    builder.scale(scale);
                } else {
                    builder.dataType(new DecimalType(precision.intValue(), DEFAULT_SCALE));
                    builder.columnLength(precision);
                    builder.scale(DEFAULT_SCALE);
                }
                break;
            case TIBERO_FLOAT:
                // The float type will be converted to DecimalType(10, -127),
                // which will lose precision in the spark engine
                DecimalType floatDecimal = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                builder.dataType(floatDecimal);
                builder.columnLength((long) floatDecimal.getPrecision());
                builder.scale(floatDecimal.getScale());
                break;
            case TIBERO_BINARY_FLOAT:
            case TIBERO_REAL:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case TIBERO_BINARY_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case TIBERO_CHAR:
            case TIBERO_VARCHAR:
            case TIBERO_VARCHAR2:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case TIBERO_NCHAR:
            case TIBERO_NVARCHAR2:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(
                        TypeDefineUtils.doubleByteTo4ByteLength(typeDefine.getLength()));
                break;
            case TIBERO_ROWID:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(MAX_ROWID_LENGTH);
                break;
            case TIBERO_XML:
            case TIBERO_SYS_XML:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case TIBERO_LONG:
                builder.dataType(BasicType.STRING_TYPE);
                // The maximum length of the column is 2GB-1
                builder.columnLength(BYTES_2GB - 1);
                break;
            case TIBERO_CLOB:
            case TIBERO_NCLOB:
                builder.dataType(BasicType.STRING_TYPE);
                // The maximum length of the column is 4GB-1
                builder.columnLength(BYTES_4GB - 1);
                break;
            case TIBERO_BLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                // The maximum length of the column is 4GB-1
                builder.columnLength(BYTES_4GB - 1);
                break;
            case TIBERO_RAW:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                if (typeDefine.getLength() == null || typeDefine.getLength() == 0) {
                    builder.columnLength(MAX_RAW_LENGTH);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case TIBERO_LONG_RAW:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                // The maximum length of the column is 2GB-1
                builder.columnLength(BYTES_2GB - 1);
                break;
            case TIBERO_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case TIBERO_TIMESTAMP:
            case TIBERO_TIMESTAMP_WITH_TIME_ZONE:
            case TIBERO_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                if (typeDefine.getScale() == null) {
                    builder.scale(TIMESTAMP_DEFAULT_SCALE);
                } else {
                    builder.scale(typeDefine.getScale());
                }
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.TIBERO, tiberoType, typeDefine.getName());
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());
        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
                builder.columnType(String.format("%s(%s)", TIBERO_NUMBER, 1));
                builder.dataType(TIBERO_NUMBER);
                builder.length(1L);
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                builder.columnType(TIBERO_INTEGER);
                builder.dataType(TIBERO_INTEGER);
                break;
            case FLOAT:
                builder.columnType(TIBERO_BINARY_FLOAT);
                builder.dataType(TIBERO_BINARY_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(TIBERO_BINARY_DOUBLE);
                builder.dataType(TIBERO_BINARY_DOUBLE);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                long precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                if (precision <= 0) {
                    precision = DEFAULT_PRECISION;
                    scale = DEFAULT_SCALE;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which is precision less than 0, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            scale);
                } else if (precision > MAX_PRECISION) {
                    scale = (int) Math.max(0, scale - (precision - MAX_PRECISION));
                    precision = MAX_PRECISION;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum precision of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_PRECISION,
                            precision,
                            scale);
                }
                if (scale < 0) {
                    scale = 0;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which is scale less than 0, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            scale);
                } else if (scale > MAX_SCALE) {
                    scale = MAX_SCALE;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_SCALE,
                            precision,
                            scale);
                }
                builder.columnType(String.format("%s(%s,%s)", TIBERO_NUMBER, precision, scale));
                builder.dataType(TIBERO_NUMBER);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(TIBERO_BLOB);
                    builder.dataType(TIBERO_BLOB);
                } else if (column.getColumnLength() <= MAX_RAW_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", TIBERO_RAW, column.getColumnLength()));
                    builder.dataType(TIBERO_RAW);
                } else {
                    builder.columnType(TIBERO_BLOB);
                    builder.dataType(TIBERO_BLOB);
                }
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(
                            String.format("%s(%s)", TIBERO_VARCHAR2, MAX_VARCHAR_LENGTH));
                    builder.dataType(TIBERO_VARCHAR2);
                } else if (column.getColumnLength() <= MAX_VARCHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", TIBERO_VARCHAR2, column.getColumnLength()));
                    builder.dataType(TIBERO_VARCHAR2);
                } else {
                    builder.columnType(TIBERO_CLOB);
                    builder.dataType(TIBERO_CLOB);
                }
                break;
            case DATE:
                builder.columnType(TIBERO_DATE);
                builder.dataType(TIBERO_DATE);
                break;
            case TIMESTAMP:
                if (column.getScale() == null || column.getScale() <= 0) {
                    builder.columnType(TIBERO_TIMESTAMP_WITH_LOCAL_TIME_ZONE);
                } else {
                    int timestampScale = column.getScale();
                    if (column.getScale() > MAX_TIMESTAMP_SCALE) {
                        timestampScale = MAX_TIMESTAMP_SCALE;
                        log.warn(
                                "The timestamp column {} type timestamp({}) is out of range, "
                                        + "which exceeds the maximum scale of {}, "
                                        + "it will be converted to timestamp({})",
                                column.getName(),
                                column.getScale(),
                                MAX_TIMESTAMP_SCALE,
                                timestampScale);
                    }
                    builder.columnType(
                            String.format("TIMESTAMP(%s) WITH LOCAL TIME ZONE", timestampScale));
                    builder.scale(timestampScale);
                }
                builder.dataType(TIBERO_TIMESTAMP_WITH_LOCAL_TIME_ZONE);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.TIBERO,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }
}
