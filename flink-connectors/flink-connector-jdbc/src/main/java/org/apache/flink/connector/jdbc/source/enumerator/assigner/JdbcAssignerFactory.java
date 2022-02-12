package org.apache.flink.connector.jdbc.source.enumerator.assigner;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;

import javax.annotation.Nullable;

import java.io.Serializable;

/** JdbcAssignerFactory. */
public interface JdbcAssignerFactory<SplitT extends JdbcSourceSplit> extends Serializable {

    default String identifier() {
        return "default";
    }

    JdbcSplitAssigner<SplitT> create(
            ReadableConfig readableConfig, @Nullable Serializable optionalSqlSplitEnumeratorState);
}
