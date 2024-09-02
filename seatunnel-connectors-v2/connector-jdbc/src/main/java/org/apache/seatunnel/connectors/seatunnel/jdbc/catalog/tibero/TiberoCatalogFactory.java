package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.tibero;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;

import java.util.Optional;

@AutoService(Factory.class)
public class TiberoCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.TIBERO;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String urlWithDatabase = options.get(JdbcCatalogOptions.BASE_URL);
        JdbcUrlUtil.UrlInfo urlInfo = TiberoURLParser.parse(urlWithDatabase);
        Optional<String> defaultDatabase = urlInfo.getDefaultDatabase();
        if (!defaultDatabase.isPresent()) {
            throw new OptionValidationException(JdbcCatalogOptions.BASE_URL);
        }
        return new TiberoCatalog(
                catalogName,
                options.get(JdbcCatalogOptions.USERNAME),
                options.get(JdbcCatalogOptions.PASSWORD),
                urlInfo,
                options.get(JdbcCatalogOptions.SCHEMA));
    }

    @Override
    public OptionRule optionRule() {
        return JdbcCatalogOptions.BASE_RULE.build();
    }
}
