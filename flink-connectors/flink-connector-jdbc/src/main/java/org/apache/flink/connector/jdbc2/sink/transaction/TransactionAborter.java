/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc2.sink.transaction;

import java.io.Closeable;
import java.io.IOException;

/**
 * Aborts lingering transactions on restart.
 *
 * <p>Transactions are lingering if they are not tracked anywhere. For example, if a job is started
 * transactions are opened. A restart without checkpoint would not allow Flink to abort old
 * transactions. Since Kafka's transactions are sequential, newly produced data does not become
 * visible for read_committed consumers. However, Kafka has no API for querying open transactions,
 * so they become lingering.
 *
 * <p>Flink solves this by assuming consecutive transaction ids. On restart of checkpoint C on
 * subtask S, it will sequentially cancel transaction C+1, C+2, ... of S until it finds the first
 * unused transaction.
 *
 * <p>Additionally, to cover for weird downscaling cases without checkpoints, it also checks for
 * transactions of subtask S+P where P is the current parallelism until it finds a subtask without
 * transactions.
 */
class TransactionAborter implements Closeable {

    @Override
    public void close() throws IOException {}
}
