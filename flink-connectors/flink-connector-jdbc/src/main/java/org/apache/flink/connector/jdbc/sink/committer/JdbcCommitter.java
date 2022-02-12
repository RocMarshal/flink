package org.apache.flink.connector.jdbc.sink.committer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterConfig;
import org.apache.flink.connector.jdbc.xa.CheckpointAndXid;
import org.apache.flink.connector.jdbc.xa.SemanticXidGenerator;
import org.apache.flink.connector.jdbc.xa.XaFacade;
import org.apache.flink.connector.jdbc.xa.XaGroupOps;
import org.apache.flink.connector.jdbc.xa.XaGroupOpsImpl;

import org.apache.flink.connector.jdbc.xa.XidGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.transaction.xa.Xid;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

/** JdbcCommitter. */
public class JdbcCommitter implements Committer<JdbcCommittable>, Serializable {

    public static final Logger LOG = LoggerFactory.getLogger(JdbcCommitter.class);

    private XaFacade xaFacade;

    private XaGroupOps xaGroupOps;
    private JdbcWriterConfig jdbcWriterConfig;

    private RuntimeContext runtimeContext;
    XidGenerator xidGenerator;

    public JdbcCommitter(RuntimeContext runtimeContext, @Nonnull JdbcWriterConfig jdbcWriterConfig) {
        this.runtimeContext = runtimeContext;
        this.jdbcWriterConfig = jdbcWriterConfig;
        if (jdbcWriterConfig.getDeliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE) {
            xaFacade = XaFacade.fromXaDataSourceSupplier(
                    jdbcWriterConfig.getJdbcConnectionOptions()
                            .getXaDatasourceSupplier(),
                    jdbcWriterConfig.getJdbcExactlyOnceOptions().getTimeoutSec(),
                    jdbcWriterConfig.getJdbcExactlyOnceOptions().isTransactionPerConnection());
            try {
                xaFacade.open();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            xaGroupOps = new XaGroupOpsImpl(xaFacade);
        }
        xidGenerator = XidGenerator.semanticXidGenerator();
        if (jdbcWriterConfig.getJdbcExactlyOnceOptions().isDiscoverAndRollbackOnRecovery()) {
            xaGroupOps.recoverAndRollback(runtimeContext, xidGenerator);
        }

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

                LOG.error("JdbcCommitter startCommit_____________, {}", commitRequest.getCommittable());
                CheckpointAndXid checkpointAndXid = commitRequest
                        .getCommittable()
                        .getCheckpointAndXid();
                LOG.error("_______Use original");
                //if (!checkpointAndXid.isRestored()) {
                    internalXaFacade.commit(checkpointAndXid.getXid(), true);
                //}

                LOG.error("JdbcCommitter endCommit_____________, {}", commitRequest.getCommittable());

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
