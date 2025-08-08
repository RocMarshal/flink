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

package org.apache.flink.runtime.webmonitor.history.retaining;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

/** Testing for {@link CompositeBasedJobRetainedStrategy}. */
class CompositeBasedJobRetainedStrategyTest {

    private static final class TestingFileStatus implements FileStatus {

        private final long modificationTime;

        TestingFileStatus(long modificationTime) {
            this.modificationTime = modificationTime;
        }

        @Override
        public long getLen() {
            return 0;
        }

        @Override
        public long getBlockSize() {
            return 0;
        }

        @Override
        public short getReplication() {
            return 0;
        }

        @Override
        public long getModificationTime() {
            return modificationTime;
        }

        @Override
        public long getAccessTime() {
            return 0;
        }

        @Override
        public boolean isDir() {
            return false;
        }

        @Override
        public Path getPath() {
            return null;
        }
    }

    // TimeToLiveBasedJobRetainedStrategy
    // QuantityBasedJobRetainedStrategy
    // 兼容性
    // 异常配置
    // 5个配置项的配置路径覆盖 + should retain 逻辑测试
}
