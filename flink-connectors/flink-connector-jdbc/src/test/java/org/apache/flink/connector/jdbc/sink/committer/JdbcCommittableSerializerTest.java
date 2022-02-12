package org.apache.flink.connector.jdbc.sink.committer;

import org.apache.flink.connector.jdbc.xa.CheckpointAndXid;
import org.apache.flink.connector.jdbc.xa.XidImpl;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * {@link JdbcCommittableSerializer} test class.
 */
public class JdbcCommittableSerializerTest {

    @Test
    void test() throws IOException {
        XidImpl xid = new XidImpl(201, "gtid".getBytes(
                StandardCharsets.UTF_8), "branch".getBytes(StandardCharsets.UTF_8));
        JdbcCommittable jdbcCommittable = new JdbcCommittable(null, CheckpointAndXid.createRestored(1L, 2, xid));
        JdbcCommittableSerializer serializer = new JdbcCommittableSerializer();
        byte[] serialize = serializer.serialize(jdbcCommittable);
        System.out.println(jdbcCommittable);
        JdbcCommittable deserialize = serializer.deserialize(serializer.getVersion(), serialize);
        System.out.println(deserialize);
        System.out.println(deserialize.equals(jdbcCommittable));
    }
}
