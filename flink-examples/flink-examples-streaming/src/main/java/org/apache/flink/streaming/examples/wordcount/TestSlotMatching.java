package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;

public class TestSlotMatching {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        conf.set(TaskManagerOptions.MINI_CLUSTER_NUM_TASK_MANAGERS, 5);
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 5);
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        environment.disableOperatorChaining();

        DataStreamSource<String> source = environment
                .addSource(
                        new RichParallelSourceFunction<String>() {
                            @Override
                            public void run(SourceContext<String> sourceContext) throws Exception {
                                while (true) {
                                    sourceContext.collect("sss");
                                }
                            }

                            @Override
                            public void cancel() {
                            }
                        })
                .setParallelism(1);
        source.name("source");//1

        //2
        source.addSink(new DiscardingSink<>()).name("sink1")
                .setParallelism(2);
        //3 4
        source.map(s->s).setParallelism(1).name("map2").addSink(new DiscardingSink<>()).name("sink2")
                .setParallelism(1);
        //5
        source.addSink(new DiscardingSink<>()).name("sink3")
                .setParallelism(1);
        environment.execute();
    }
}

