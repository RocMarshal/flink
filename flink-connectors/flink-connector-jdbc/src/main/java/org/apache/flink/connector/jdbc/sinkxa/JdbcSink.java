package org.apache.flink.connector.jdbc.sinkxa;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.sinkxa.committer.JdbcCommittable;
import org.apache.flink.connector.jdbc.sinkxa.committer.JdbcCommittableSerializer;
import org.apache.flink.connector.jdbc.sinkxa.committer.JdbcCommitter;
import org.apache.flink.connector.jdbc.sinkxa.writer.JdbcWriter;
import org.apache.flink.connector.jdbc.sinkxa.writer.JdbcWriterConfig;
import org.apache.flink.connector.jdbc.sinkxa.writer.JdbcWriterState;
import org.apache.flink.connector.jdbc.sinkxa.writer.JdbcWriterStateSerializer;
import org.apache.flink.connector.jdbc.xa.CheckpointAndXid;
import org.apache.flink.connector.jdbc.xa.XaFacade;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.function.SerializableSupplier;

import javax.annotation.Nullable;
import javax.sql.XADataSource;
import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

// JdbcSink class.
public class JdbcSink<IN>
        implements StatefulSink<IN, JdbcWriterState>, TwoPhaseCommittingSink<IN, JdbcCommittable> {

    private JdbcStatementBuilder<IN> statementBuilder;
    private JdbcExecutionOptions executionOptions;
    SerializableSupplier<XADataSource> dataSourceSupplier;
    JdbcExactlyOnceOptions exactlyOnceOptions;
    private String sql;
    private TypeInformation<?> typeInformation;

    private XaFacade xaFacade;

    public JdbcSink(
            String sql,
            JdbcStatementBuilder<IN> statementBuilder,
            SerializableSupplier<XADataSource> dataSourceSupplier,
            JdbcExecutionOptions executionOptions,
            JdbcExactlyOnceOptions exactlyOnceOptions,
            TypeInformation<?> typeInformation) {
        this.sql = sql;
        this.statementBuilder = statementBuilder;
        this.dataSourceSupplier = dataSourceSupplier;
        this.exactlyOnceOptions = exactlyOnceOptions;
        this.executionOptions = executionOptions;
        this.typeInformation = typeInformation;
    }

    @Override
    public JdbcWriter<IN> createWriter(InitContext context) throws IOException {

        if (xaFacade == null) {
            this.xaFacade =
                    XaFacade.fromXaDataSourceSupplier(
                            dataSourceSupplier,
                            exactlyOnceOptions.getTimeoutSec(),
                            exactlyOnceOptions.isTransactionPerConnection());
        }

        return new JdbcWriter<>(
                sql,
                statementBuilder,
                xaFacade,
                executionOptions,
                exactlyOnceOptions,
                context,
                JdbcWriterState.empty(),
                typeInformation);
    }

    @Override
    public Committer<JdbcCommittable> createCommitter() throws IOException {
        if (xaFacade == null) {
            this.xaFacade =
                    XaFacade.fromXaDataSourceSupplier(
                            dataSourceSupplier,
                            exactlyOnceOptions.getTimeoutSec(),
                            exactlyOnceOptions.isTransactionPerConnection());
        }
        return new JdbcCommitter(
                JdbcWriterConfig.builder()
                        //                        .setJdbcConnectionOptions()
                        .setJdbcExactlyOnceOptions(exactlyOnceOptions)
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setJdbcExecutionOptions(executionOptions)
                        .build(),
                xaFacade);
    }

    @Override
    public SimpleVersionedSerializer<JdbcCommittable> getCommittableSerializer() {
        return new JdbcCommittableSerializer();
    }

    @Override
    public StatefulSinkWriter<IN, JdbcWriterState> restoreWriter(
            InitContext context, Collection<JdbcWriterState> recoveredState) {
        return new JdbcWriter<>(
                sql,
                statementBuilder,
                XaFacade.fromXaDataSourceSupplier(
                        dataSourceSupplier,
                        exactlyOnceOptions.getTimeoutSec(),
                        exactlyOnceOptions.isTransactionPerConnection()),
                executionOptions,
                exactlyOnceOptions,
                context,
                merge(recoveredState),
                typeInformation);
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
