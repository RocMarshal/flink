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

package org.apache.flink.state.benchmark;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.state.rocksdb.RocksDBKeyedStateBackend;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.applyToAllKeys;
import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.cleanUp;
import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.compactState;
import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.createKeyedStateBackend;
import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.getListState;
import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.getMapState;
import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.getValueState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Test for {@link StateBackendBenchmarkUtils}. */
@ExtendWith(ParameterizedTestExtension.class)
class StateBackendBenchmarkUtilsTest {
    private final ValueStateDescriptor<Long> valueStateDescriptor =
            new ValueStateDescriptor<>("valueState", Long.class);
    private final ListStateDescriptor<Long> listStateDescriptor =
            new ListStateDescriptor<>("listState", Long.class);
    private final MapStateDescriptor<Long, Double> mapStateDescriptor =
            new MapStateDescriptor<>("mapState", Long.class, Double.class);

    @Parameters(name = "{0}")
    private static Collection<StateBackendBenchmarkUtils.StateBackendType> data() {
        return Arrays.asList(StateBackendBenchmarkUtils.StateBackendType.values());
    }

    @Parameter private StateBackendBenchmarkUtils.StateBackendType backendType;

    @TestTemplate
    void testCreateKeyedStateBackend() throws IOException {
        KeyedStateBackend<Long> backend = createKeyedStateBackend(backendType);
        cleanUp(backend);
    }

    @TestTemplate
    void testGetValueState() throws Exception {
        KeyedStateBackend<Long> backend = createKeyedStateBackend(backendType);
        getValueState(backend, valueStateDescriptor);
        cleanUp(backend);
    }

    @TestTemplate
    void testGetListState() throws Exception {
        KeyedStateBackend<Long> backend = createKeyedStateBackend(backendType);
        getListState(backend, listStateDescriptor);
        cleanUp(backend);
    }

    @TestTemplate
    void testGetMapState() throws Exception {
        KeyedStateBackend<Long> backend = createKeyedStateBackend(backendType);
        getMapState(backend, mapStateDescriptor);
        cleanUp(backend);
    }

    @TestTemplate
    void testApplyToAllKeys() throws Exception {
        assumeTrue(backendType != StateBackendBenchmarkUtils.StateBackendType.BATCH_EXECUTION);
        KeyedStateBackend<Long> backend = createKeyedStateBackend(backendType);
        ListState<Long> listState = getListState(backend, listStateDescriptor);
        for (long i = 0; i < 10; i++) {
            backend.setCurrentKey(i);
            listState.add(i);
        }
        applyToAllKeys(
                backend,
                listStateDescriptor,
                (k, state) -> {
                    backend.setCurrentKey(k);
                    state.clear();
                });
        for (long i = 0; i < 10; i++) {
            backend.setCurrentKey(i);
            assertThat(listState.get()).isNull();
        }
        cleanUp(backend);
    }

    @TestTemplate
    void testCompactState() throws Exception {
        KeyedStateBackend<Long> backend = createKeyedStateBackend(backendType);
        ListState<Long> listState = getListState(backend, listStateDescriptor);
        for (long i = 0; i < 10; i++) {
            backend.setCurrentKey(i);
            listState.add(i);
        }
        if (backend instanceof RocksDBKeyedStateBackend) {
            RocksDBKeyedStateBackend<Long> rocksDBKeyedStateBackend =
                    (RocksDBKeyedStateBackend<Long>) backend;
            compactState(rocksDBKeyedStateBackend, listStateDescriptor);
        }
        cleanUp(backend);
    }
}
