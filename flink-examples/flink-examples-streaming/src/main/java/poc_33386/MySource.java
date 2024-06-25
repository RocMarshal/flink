package poc_33386;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;

public class MySource extends RichParallelSourceFunction<String> {
    volatile boolean running = true;
    boolean error;
    int count = 0;
    int batchInterval;
    int subTask;

    public MySource(boolean error, int batchInterval, int subTask) {
        this.error = error;
        this.batchInterval = batchInterval;
        this.subTask = subTask;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(UUID.randomUUID().toString());
            count++;
            Thread.sleep(1000L);

            if (error) {
                if (count % batchInterval == 0
                        && getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() == subTask) {
                    throw new RuntimeException("Source Manual Exception from " + subTask);
                }
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
