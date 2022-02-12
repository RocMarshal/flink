package org.apache.flink.connector.jdbc.source.enumerator.assigner;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

/** JdbcSourceSplitBetweenParametersAssigner. */
public class JdbcSourceSplitBetweenParametersAssigner extends JdbcSplitAssignerBase<JdbcSourceSplit>
        implements JdbcSplitAssigner<JdbcSourceSplit> {

    private final long minVal;
    private final long maxVal;

    private long batchSize;
    private int batchNum;

    private String sqlTemplate;

    private State state = new State();

    @Nullable
    @Override
    public Serializable getOptionalUserDefinedState() {
        return this.state;
    }

    @Override
    public void setOptionalUserDefinedState(@Nullable Serializable optionalUserDefinedState) {
        if (optionalUserDefinedState == null) {
            this.state = new State();
            return;
        }
        this.state = (State) optionalUserDefinedState;
    }

    static class State implements Serializable {
        @Nonnull ArrayList<JdbcSourceSplit> pendingSplits = new ArrayList<>();
        int parametersIndex = 0;
    }

    /**
     * JdbcSourceSplitBetweenParametersAssigner constructor.
     *
     * @param minVal the lower bound of the produced "from" values
     * @param maxVal the upper bound of the produced "to" values
     */
    public JdbcSourceSplitBetweenParametersAssigner(long minVal, long maxVal) {
        super(new Configuration());
        Preconditions.checkArgument(minVal <= maxVal, "minVal must not be larger than maxVal");
        this.minVal = minVal;
        this.maxVal = maxVal;
    }

    public String getSqlTemplate() {
        return sqlTemplate;
    }

    public void setSqlTemplate(String sqlTemplate) {
        this.sqlTemplate = Preconditions.checkNotNull(sqlTemplate);
    }

    /**
     * JdbcSourceSplitBetweenParametersAssigner constructor.
     *
     * @param fetchSize the max distance between the produced from/to pairs
     * @param minVal the lower bound of the produced "from" values
     * @param maxVal the upper bound of the produced "to" values
     */
    public JdbcSourceSplitBetweenParametersAssigner(long fetchSize, long minVal, long maxVal) {
        super(new Configuration());
        Preconditions.checkArgument(minVal <= maxVal, "minVal must not be larger than maxVal");
        this.minVal = minVal;
        this.maxVal = maxVal;
        ofBatchSize(fetchSize);
    }

    public JdbcSourceSplitBetweenParametersAssigner ofBatchSize(long batchSize) {
        Preconditions.checkArgument(batchSize > 0, "Batch size must be positive");

        long maxElemCount = (maxVal - minVal) + 1;
        if (batchSize > maxElemCount) {
            batchSize = maxElemCount;
        }
        this.batchSize = batchSize;
        this.batchNum = new Double(Math.ceil((double) maxElemCount / batchSize)).intValue();
        return this;
    }

    public JdbcSourceSplitBetweenParametersAssigner ofBatchNum(int batchNum) {
        Preconditions.checkArgument(batchNum > 0, "Batch number must be positive");

        long maxElemCount = (maxVal - minVal) + 1;
        if (batchNum > maxElemCount) {
            batchNum = (int) maxElemCount;
        }
        this.batchNum = batchNum;
        this.batchSize = new Double(Math.ceil((double) maxElemCount / batchNum)).longValue();
        return this;
    }

    private Serializable[][] getParameterValues() {
        Preconditions.checkState(
                batchSize > 0,
                "Batch size and batch number must be positive. Have you called `ofBatchSize` or `ofBatchNum`?");

        long maxElemCount = (maxVal - minVal) + 1;
        long bigBatchNum = maxElemCount - (batchSize - 1) * batchNum;

        Serializable[][] parameters = new Serializable[batchNum][2];
        long start = minVal;
        for (int i = 0; i < batchNum; i++) {
            long end = start + batchSize - 1 - (i >= bigBatchNum ? 1 : 0);
            parameters[i] = new Long[] {start, end};
            start = end + 1;
        }
        return parameters;
    }

    @Override
    public void open(ReadableConfig config) {}

    @Override
    public Optional<JdbcSourceSplit> getNextSplit() {
        if (!state.pendingSplits.isEmpty()) {
            Iterator<JdbcSourceSplit> iterator = state.pendingSplits.iterator();
            JdbcSourceSplit split = iterator.next();
            iterator.remove();
            return Optional.of(split);
        }
        if (getParameterValues().length == 0) {
            return Optional.empty();
        }
        int len = getParameterValues().length;
        if (state.parametersIndex < 0) {
            state.parametersIndex = 0;
        }
        int index = state.parametersIndex;
        if (index >= len) {
            return Optional.empty();
        }
        Optional<JdbcSourceSplit> split =
                Optional.of(
                        new JdbcSourceSplit(
                                getNextSplitId(), sqlTemplate, getParameterValues()[index], 0));
        if (index < len - 1) {
            state.parametersIndex = index + 1;
        }
        return split;
    }

    @Override
    public void addSplits(Collection<JdbcSourceSplit> splits) {
        this.state.pendingSplits.addAll(splits);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void close() {}
}
