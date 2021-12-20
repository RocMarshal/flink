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

package org.apache.flink.connector.clickhouse.internal.partitioner;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;

/** ClickHouse data partitioner interface. */
public interface ClickHousePartitioner extends Partitioner<RowData> {

    String BALANCED = "balanced";

    String SHUFFLE = "shuffle";

    String HASH = "hash";

    int partition(RowData record, int numShards);

    static ClickHousePartitioner createBalanced() {
        return new BalancedPartitioner();
    }

    static ClickHousePartitioner createShuffle() {
        return new ShufflePartitioner();
    }

    static ClickHousePartitioner createHash(FieldGetter getter) {
        return new HashPartitioner(getter);
    }
}
