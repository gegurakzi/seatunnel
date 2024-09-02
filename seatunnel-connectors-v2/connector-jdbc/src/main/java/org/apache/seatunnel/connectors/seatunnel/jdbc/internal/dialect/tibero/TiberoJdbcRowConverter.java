package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.tibero;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

public class TiberoJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.TIBERO;
    }
}
