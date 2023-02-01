package org.apache.flink.connector.jdbc.sinkxa;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.jdbc.sinkxa.committer.JdbcCommittable;
import org.apache.flink.connector.jdbc.sinkxa.writer.JdbcWriter;
import org.apache.flink.connector.jdbc.sinkxa.writer.JdbcWriterState;
import org.apache.flink.connector.jdbc.sinkxa.writer.JdbcWriterStateSerializer;
import org.apache.flink.connector.jdbc.xa.CheckpointAndXid;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import javax.annotation.Nullable;
import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

// JdbcSink class.
public class JdbcSink<IN>
        implements StatefulSink<IN, JdbcWriterState>, TwoPhaseCommittingSink<IN, JdbcCommittable>{

    @Override
    public PrecommittingSinkWriter<IN, JdbcCommittable> createWriter(InitContext context) throws IOException {

        return new JdbcWriter<>(
                context,
                JdbcWriterState.empty(),
                null,
                null,
                null,
                null,
                null);;
    }

    @Override
    public Committer<JdbcCommittable> createCommitter() throws IOException {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<JdbcCommittable> getCommittableSerializer() {
        return null;
    }

    @Override
    public StatefulSinkWriter<IN, JdbcWriterState> restoreWriter(
            InitContext context, Collection<JdbcWriterState> recoveredState) {
        return new JdbcWriter<>(
                context,
                merge(recoveredState),
                null,
                null,
                null,
                null,
                null);
    }

    private JdbcWriterState merge(@Nullable Iterable<JdbcWriterState> states) {
        if (states == null) {
            return JdbcWriterState.empty();
        }
        List<Xid> hanging = new ArrayList<>();
        List<CheckpointAndXid> prepared = new ArrayList<>();
        states.forEach(
                i -> {
                    hanging.addAll(i.getHanging());
                    prepared.addAll(i.getPrepared());
                });
        return JdbcWriterState.of(prepared, hanging);
    }

    @Override
    public SimpleVersionedSerializer<JdbcWriterState> getWriterStateSerializer() {
        return new JdbcWriterStateSerializer();
    }
}
