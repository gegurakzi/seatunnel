package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.tibero;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;

import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class TiberoDialectFactory implements JdbcDialectFactory {
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:tibero:thin:");
    }

    @Override
    public JdbcDialect create() {
        return new TiberoDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new TiberoDialect(fieldIde);
    }
}
