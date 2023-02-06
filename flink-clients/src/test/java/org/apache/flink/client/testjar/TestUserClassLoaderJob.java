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

package org.apache.flink.client.testjar;

/** This class can used to test situation that the jar is not in the system classpath. */
public class TestUserClassLoaderJob {
    public static void main(String[] args) throws Exception {
        /*final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<Integer> source =
                env.fromElements(new TestUserClassLoaderJobLib().getValue(), 1, 2, 3, 4);
        final SingleOutputStreamOperator<Integer> mapper = source.map(element -> 2 * element);
        mapper.addSink(new DiscardingSink<>());

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        env.execute(
                TestUserClassLoaderJob.class.getCanonicalName()
                        + "-"
                        + parameterTool.getRequired("arg"));*/
    }
}
