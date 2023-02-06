package org.apache.flink.connector.jdbc.sinkxa.committer;

import org.apache.flink.connector.jdbc.xa.CheckpointAndXid;
import org.apache.flink.connector.jdbc.xa.XaFacade;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** JdbcCommittable. */
public class JdbcCommittable implements Serializable {

    private @Nullable XaFacade xaFacade;
    private @Nonnull CheckpointAndXid checkpointAndXid;

    public JdbcCommittable(XaFacade xaFacade, @Nonnull CheckpointAndXid checkpointAndXid) {
        this.xaFacade = xaFacade;
        this.checkpointAndXid = checkpointAndXid;
    }

    public XaFacade getXaFacade() {
        return xaFacade;
    }

    public CheckpointAndXid getCheckpointAndXid() {
        return checkpointAndXid;
    }

    public void setCheckpointAndXid(CheckpointAndXid checkpointAndXid) {
        this.checkpointAndXid = checkpointAndXid;
    }

    @Override
    public String toString() {
        return "JdbcCommittable{"
                + "xaFacade="
                + xaFacade
                + ", checkpointAndXid="
                + checkpointAndXid
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JdbcCommittable that = (JdbcCommittable) o;
        return Objects.equals(xaFacade, that.xaFacade)
                && checkpointAndXid.equals(that.checkpointAndXid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(xaFacade, checkpointAndXid);
    }
}
