package org.apache.flink.connector.jdbc.sinkxa.committer;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.jdbc.sinkxa.writer.JdbcWriterConfig;
import org.apache.flink.connector.jdbc.xa.CheckpointAndXid;
import org.apache.flink.connector.jdbc.xa.XaFacade;
import org.apache.flink.connector.jdbc.xa.XaGroupOps;
import org.apache.flink.connector.jdbc.xa.XaGroupOpsImpl;
import org.apache.flink.connector.jdbc.xa.XidGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

/** JdbcCommitter. */
public class JdbcCommitter implements Committer<JdbcCommittable>, Serializable {

    public static final Logger LOG = LoggerFactory.getLogger(JdbcCommitter.class);

    private XaFacade xaFacade;

    private XaGroupOps xaGroupOps;
    private JdbcWriterConfig jdbcWriterConfig;

    private RuntimeContext runtimeContext;
    XidGenerator xidGenerator;

    public JdbcCommitter(@Nonnull JdbcWriterConfig jdbcWriterConfig, XaFacade xaFacade) {
        this.jdbcWriterConfig = jdbcWriterConfig;
        this.xaFacade = xaFacade;
        if (!this.xaFacade.isOpen()) {
            try {
                this.xaFacade.open();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        this.xaGroupOps = new XaGroupOpsImpl(xaFacade);

        // xidGenerator = XidGenerator.semanticXidGenerator();
        //        if
        // (jdbcWriterConfig.getJdbcExactlyOnceOptions().isDiscoverAndRollbackOnRecovery()) {
        //            xaGroupOps.recoverAndRollback(runtimeContext, xidGenerator);
        //        }
        LOG.error("_____jdbcCommitter");
    }

    @Override
    public void commit(Collection<CommitRequest<JdbcCommittable>> committables)
            throws IOException, InterruptedException {
        if (committables == null || committables.isEmpty()) {
            return;
        }
        for (CommitRequest<JdbcCommittable> commitRequest : committables) {
            try {
                XaFacade internalXaFacade = commitRequest.getCommittable().getXaFacade();
                if (internalXaFacade == null || !internalXaFacade.isOpen()) {
                    LOG.error("internalXaFacade in recover.......____________-");
                    internalXaFacade = xaFacade;
                    if (!internalXaFacade.isOpen()) {
                        internalXaFacade.open();
                    }
                } else {
                    this.xaFacade = internalXaFacade;
                }

                LOG.error(
                        "JdbcCommitter startCommit_____________, {}",
                        commitRequest.getCommittable());
                CheckpointAndXid checkpointAndXid =
                        commitRequest.getCommittable().getCheckpointAndXid();
                LOG.error("_______Use original");
                // if (!checkpointAndXid.isRestored()) {
                xaGroupOps.commit(Collections.singletonList(checkpointAndXid), true, 3);
                // }

                LOG.error(
                        "JdbcCommitter endCommit_____________, {}", commitRequest.getCommittable());

            } catch (XaFacade.TransientXaException e) {
                if (commitRequest.getNumberOfRetries()
                        >= jdbcWriterConfig.getJdbcExactlyOnceOptions().getMaxCommitAttempts()) {
                    commitRequest.signalFailedWithKnownReason(e);
                } else {
                    commitRequest.retryLater();
                }
                LOG.error("___+_+_+_", e);
            } catch (Exception e) {
                LOG.error("JdbcCommitter 2_____________, {}", commitRequest.getCommittable());
                LOG.error("JdbcCommitter error2_____________.", e);
                LOG.error("____++++{}", e.getClass());
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        xaFacade.close();
    }
}
