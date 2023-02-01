package org.apache.flink.connector.jdbc.sinkxa.writer;

import org.apache.flink.connector.jdbc.xa.CheckpointAndXid;

import javax.transaction.xa.Xid;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableCollection;

/** JdbcWriterState. */
public class JdbcWriterState implements Serializable {
    private final Collection<CheckpointAndXid> prepared;
    private final Collection<Xid> hanging;

    public static JdbcWriterState empty() {
        return new JdbcWriterState(Collections.emptyList(), Collections.emptyList());
    }

    public static JdbcWriterState of(
            Collection<CheckpointAndXid> prepared, Collection<Xid> hanging) {
        return new JdbcWriterState(
                unmodifiableCollection(new ArrayList<>(prepared)),
                unmodifiableCollection(new ArrayList<>(hanging)));
    }

    private JdbcWriterState(Collection<CheckpointAndXid> prepared, Collection<Xid> hanging) {
        this.prepared = prepared;
        this.hanging = hanging;
    }

    public JdbcWriterState merge(JdbcWriterState jdbcWriterState) {
        if (Objects.isNull(jdbcWriterState)) {
            return this;
        }
        List<CheckpointAndXid> checkpointAndXidList = new ArrayList<>(prepared);
        List<Xid> xids = new ArrayList<>(hanging);
        if (Objects.nonNull(jdbcWriterState.getHanging())) {
            xids.addAll(jdbcWriterState.getHanging());
        }
        if (Objects.nonNull(jdbcWriterState.getPrepared())) {
            checkpointAndXidList.addAll(jdbcWriterState.getPrepared());
        }

        return JdbcWriterState.of(new ArrayList<>(), xids);
    }

    /**
     * @return immutable collection of prepared XA transactions to {@link
     *     javax.transaction.xa.XAResource#commit commit}.
     */
    public Collection<CheckpointAndXid> getPrepared() {
        return prepared;
    }

    /**
     * @return immutable collection of XA transactions to {@link
     *     javax.transaction.xa.XAResource#rollback rollback} (if they were prepared) or {@link
     *     javax.transaction.xa.XAResource#end end} (if they were only started).
     */
    public Collection<Xid> getHanging() {
        return hanging;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JdbcWriterState)) {
            return false;
        }
        JdbcWriterState that = (JdbcWriterState) o;
        return Objects.equals(getPrepared(), that.getPrepared())
                && Objects.equals(getHanging(), that.getHanging());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPrepared(), getHanging());
    }

    @Override
    public String toString() {
        return "prepared=" + prepared + ", hanging=" + hanging;
    }
}
