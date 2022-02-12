package org.apache.flink.connector.jdbc.source.enumerator.assigner;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.base.OptionalUserDefinedState;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/** JdbcSplitAssigner. */
public interface JdbcSplitAssigner<SplitT extends JdbcSourceSplit>
        extends Serializable, OptionalUserDefinedState {
    /**
     * Called to open the assigner to acquire any resources, like threads or network connections.
     * Integer splitCapacity,
     */
    void open(ReadableConfig config);

    /**
     * Gets the next split.
     *
     * <p>When this method returns an empty {@code Optional}, then the set of splits is assumed to
     * be done and the source will finish once the readers finished their current splits.
     */
    Optional<JdbcSourceSplit> getNextSplit();

    /**
     * Adds a set of splits to this assigner. This happens for example when some split processing
     * failed and the splits need to be re-added.
     */
    void addSplits(Collection<JdbcSourceSplit> splits);

    /**
     * Notifies the listener that the checkpoint with the given {@code checkpointId} completed and
     * was committed.
     *
     * @see CheckpointListener#notifyCheckpointComplete(long)
     */
    void notifyCheckpointComplete(long checkpointId);

    default Serializable snapshotState(long checkpointId) {
        return getOptionalUserDefinedState();
    }

    /**
     * Called to close the assigner, in case it holds on to any resources, like threads or network
     * connections.
     */
    void close();

    char[] getCurrentSplitId();

    default String getNextSplitId() {
        // because we just increment numbers, we increment the char representation directly,
        // rather than incrementing an integer and converting it to a string representation
        // every time again (requires quite some expensive conversion logic).
        incrementCharArrayByOne(getCurrentSplitId(), getCurrentSplitId().length - 1);
        return new String(getCurrentSplitId());
    }

    static void incrementCharArrayByOne(char[] array, int pos) {
        char c = array[pos];
        c++;

        if (c > '9') {
            c = '0';
            incrementCharArrayByOne(array, pos - 1);
        }
        array[pos] = c;
    }
}
