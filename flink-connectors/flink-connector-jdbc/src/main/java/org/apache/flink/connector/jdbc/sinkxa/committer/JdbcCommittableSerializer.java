package org.apache.flink.connector.jdbc.sinkxa.committer;

import org.apache.flink.connector.jdbc.xa.CheckpointAndXid;
import org.apache.flink.connector.jdbc.xa.XidImpl;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.transaction.xa.Xid;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** JdbcCommittableSerializer. */
public class JdbcCommittableSerializer implements SimpleVersionedSerializer<JdbcCommittable> {

    public static final Logger LOG = LoggerFactory.getLogger(JdbcCommittableSerializer.class);

    public static final int DEFAULT_VERSION = 0;

    @Override
    public int getVersion() {
        return DEFAULT_VERSION;
    }

    @Override
    public byte[] serialize(JdbcCommittable jdbcCommittable) throws IOException {
        LOG.error("serialize_______, {}", jdbcCommittable);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
            CheckpointAndXid checkpointAndXid = jdbcCommittable.getCheckpointAndXid();
            serializeXid(dos, checkpointAndXid.getXid());
            dos.writeInt(checkpointAndXid.getAttempts());
            dos.writeLong(checkpointAndXid.getCheckpointId());
            dos.writeBoolean(checkpointAndXid.isRestored());
            dos.flush();
            return baos.toByteArray();
        }
    }

    private void serializeXid(DataOutputStream dos, @Nonnull Xid xid) throws IOException {
        byte[] branchQualifier = xid.getBranchQualifier();
        byte[] globalTransactionId = xid.getGlobalTransactionId();
        int formatId = xid.getFormatId();
        dos.writeInt(formatId);
        dos.writeInt(branchQualifier.length);
        dos.write(branchQualifier);
        dos.writeInt(globalTransactionId.length);
        dos.write(globalTransactionId);
    }

    private Xid deserializeXid(DataInputStream dis) throws IOException {
        int formatId = dis.readInt();
        int branchQualifierLen = dis.readInt();
        byte[] branchQualifier = new byte[branchQualifierLen];
        dis.read(branchQualifier);
        int globalTransactionIdLen = dis.readInt();
        byte[] globalTransactionId = new byte[globalTransactionIdLen];
        dis.read(globalTransactionId);
        return new XidImpl(formatId, globalTransactionId, branchQualifier);
    }

    @Override
    public JdbcCommittable deserialize(int version, byte[] serialized) throws IOException {
        Preconditions.checkArgument(version == getVersion(), "Invalid serializer version.");
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream dis = new DataInputStream(bais)) {
            Xid xid = deserializeXid(dis);
            int attempts = dis.readInt();
            long chkId = dis.readLong();
            boolean restored = dis.readBoolean();
            CheckpointAndXid checkpointAndXid = CheckpointAndXid.createRestored(chkId,attempts, xid);

            JdbcCommittable jdbcCommittable = new JdbcCommittable(null, CheckpointAndXid.createRestored(chkId, attempts, xid));
            LOG.error("deserialize_______, {} , checkpointAndXid______, {}", jdbcCommittable, checkpointAndXid);
            return jdbcCommittable;
        }
    }
}
