package org.apache.flink.connector.jdbc.internal.connection;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.io.Serializable;

/** Connection provider factory. */
public interface JdbcConnectionProviderFactory extends Serializable {

    /** Returns a unique identifier among same factory interfaces. */
    String factoryIdentifier();

    JdbcConnectionProvider create(
            DynamicTableFactory.Context context, ReadableConfig formatOptions);
}
