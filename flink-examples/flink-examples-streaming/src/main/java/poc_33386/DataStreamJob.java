/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package poc_33386;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the tutorials and examples on the <a
 * href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run 'mvn clean package' on the
 * command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 2);
        conf.set(TaskManagerOptions.MINI_CLUSTER_NUM_TASK_MANAGERS, 5);
        conf.set(
                TaskManagerOptions.TASK_MANAGER_LOAD_BALANCE_MODE,
                TaskManagerOptions.TaskManagerLoadBalanceMode.TASKS);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setRestartStrategy(RestartStrategies.fallBackRestart());
        env.enableCheckpointing(5000L);
        boolean sourceManualError = false;
        boolean sinkManualError = false;
        args = new String[] {"source", "30", "2"};
        int recordsIntervalCount = 1800;
        int subIndex = 0;
        if (args != null && args.length == 3) {
            if (args[0].equals("source")) {
                sourceManualError = true;
            }
            if (args[0].equals("sink")) {
                sinkManualError = true;
            }
            recordsIntervalCount = Integer.parseInt(args[1]);
            subIndex = Integer.parseInt(args[2]);
        }

        MySource source = new MySource(sourceManualError, recordsIntervalCount, subIndex);

        env.setParallelism(6);
        env.addSource(source)
                .setParallelism(5)
                .rebalance()
                .addSink(new MySink(sinkManualError, recordsIntervalCount, subIndex))
                .setParallelism(10);
        // Execute program, beginning computation.
        env.execute("POC");
    }
}
