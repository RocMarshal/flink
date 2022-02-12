package org.apache.flink.connector.jdbc.sink.writer;

import org.apache.flink.connector.jdbc.xa.CheckpointAndXid;
import org.apache.flink.connector.jdbc.xa.XidImpl;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import javax.annotation.Nonnull;
import javax.transaction.xa.Xid;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Serializer for {@link JdbcWriterState}. */
public class JdbcWriterStateSerializer
        implements SimpleVersionedSerializer<JdbcWriterState>, Serializable {

    public static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(@Nonnull JdbcWriterState writerState) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            Collection<Xid> hanging = writerState.getHanging();
            Collection<CheckpointAndXid> prepared = writerState.getPrepared();
            out.writeInt(hanging.size());
            for (Xid xid : hanging) {
                writeXid(out, xid);
            }

            out.writeInt(prepared.size());
            for (CheckpointAndXid checkpointAndXid : prepared) {
                writeCheckpointAndXid(out, checkpointAndXid);
            }

            out.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public JdbcWriterState deserialize(int version, byte[] serialized) throws IOException {

        if (VERSION != version) {
            throw new IOException("Unknown version: " + version);
        }
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(byteArrayInputStream)) {
            int hangingSize = in.readInt();
            List<Xid> hanging = new ArrayList<>(hangingSize);
            for (int i = 0; i < hangingSize; i++) {
                hanging.add(readXid(in));
            }
            int chkXidSize = in.readInt();
            List<CheckpointAndXid> prepared = new ArrayList<>(chkXidSize);
            for (int i = 0; i < chkXidSize; i++) {
                prepared.add(readCheckpointAndXid(in));
            }
            return JdbcWriterState.of(new ArrayList<>(), hanging);
        }
    }

    private void writeXid(DataOutputStream out, Xid xid) throws IOException {
        out.writeInt(xid.getFormatId());
        out.writeInt(xid.getGlobalTransactionId().length);
        out.write(xid.getGlobalTransactionId());
        out.write(xid.getBranchQualifier().length);
        out.write(xid.getBranchQualifier());
    }

    private Xid readXid(DataInputStream in) throws IOException {
        int formatId = in.readInt();
        int gtidSize = in.readInt();
        byte[] gtid = new byte[gtidSize];
        in.read(gtid);
        int bqSize = in.readInt();
        byte[] bq = new byte[bqSize];
        in.read(bq);
        return new XidImpl(formatId, gtid, bq);
    }

    private void writeCheckpointAndXid(DataOutputStream out, CheckpointAndXid checkpointAndXid)
            throws IOException {
        out.writeLong(checkpointAndXid.getCheckpointId());
        out.writeInt(checkpointAndXid.getAttempts());
        writeXid(out, checkpointAndXid.getXid());
    }

    private CheckpointAndXid readCheckpointAndXid(DataInputStream in) throws IOException {
        long chkId = in.readLong();
        int attempts = in.readInt();
        Xid xid = readXid(in);
        return CheckpointAndXid.createRestored(chkId, attempts, xid);
    }
}
