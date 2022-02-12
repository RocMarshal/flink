package org.apache.flink.connector.jdbc.source.enumerator.assigner;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/** DefaultJdbcSplitAssigner. */
public class DefaultJdbcSplitAssigner extends JdbcSplitAssignerBase<JdbcSourceSplit>
        implements JdbcSplitAssigner<JdbcSourceSplit> {
    public DefaultJdbcSplitAssigner(ReadableConfig config) {
        super(config);
    }

    @Nullable
    @Override
    public Serializable getOptionalUserDefinedState() {
        return null;
    }

    @Override
    public void setOptionalUserDefinedState(@Nullable Serializable optionalUserDefinedState) {}

    @Override
    public void open(ReadableConfig config) {}

    @Override
    public Optional<JdbcSourceSplit> getNextSplit() {
        return Optional.empty();
    }

    @Override
    public void addSplits(Collection<JdbcSourceSplit> splits) {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void close() {}
}
