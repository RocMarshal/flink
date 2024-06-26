package poc_33386;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MySink extends RichSinkFunction<String> {

    boolean error = false;
    int count = 0;
    int batchInterval;
    int subTask = 0;

    public MySink(boolean error, int batchInterval, int subTask) {
        this.error = error;
        this.batchInterval = batchInterval;
        this.subTask = subTask;
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        count++;

        if (error) {
            if (count % batchInterval == 0
                    && getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() == subTask) {
                throw new RuntimeException("Sink Manual Exception from " + subTask);
            }
        }
    }
}
