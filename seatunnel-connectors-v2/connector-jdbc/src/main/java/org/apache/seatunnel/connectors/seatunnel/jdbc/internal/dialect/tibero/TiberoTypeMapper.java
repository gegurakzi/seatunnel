package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.tibero;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

@Slf4j
public class TiberoTypeMapper implements JdbcDialectTypeMapper {

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return new TiberoTypeConverter().convert(typeDefine);
    }

    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnName = metadata.getColumnLabel(colIndex);
        String nativeType = metadata.getColumnTypeName(colIndex);
        int isNullable = metadata.isNullable(colIndex);
        long precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        if ("number".equalsIgnoreCase(nativeType) && scale == -127) {
            nativeType = "float";
        } else if (Arrays.asList("NVARCHAR2", "NCHAR").contains(nativeType)) {
            long doubleByteLength = TypeDefineUtils.charToDoubleByteLength(precision);
            precision = doubleByteLength;
        } else if (Arrays.asList("CHAR", "VARCHAR", "VARCHAR2").contains(nativeType)) {
            long octetByteLength = TypeDefineUtils.charTo4ByteLength(precision);
            precision = octetByteLength;
        }

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(nativeType)
                        .dataType(nativeType)
                        .nullable(isNullable == ResultSetMetaData.columnNullable)
                        .length(precision)
                        .precision(precision)
                        .scale(scale)
                        .build();
        return mappingColumn(typeDefine);
    }
}
