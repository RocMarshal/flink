/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.batch.tests.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileBasedOneShotLatch}. */
class FileBasedOneShotLatchTest {

    private FileBasedOneShotLatch latch;

    private File latchFile;

    @BeforeEach
    void setUp(@TempDir File temporaryFolder) throws IOException {
        latchFile = new File(temporaryFolder, "latchFile");
        latch = new FileBasedOneShotLatch(latchFile.toPath());
        latchFile.createNewFile();
    }

    @Test
    void awaitReturnsWhenFileIsCreated() throws Exception {
        final AtomicBoolean awaitCompleted = new AtomicBoolean();
        final Thread thread =
                new Thread(
                        () -> {
                            try {
                                latch.await();
                                awaitCompleted.set(true);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
        thread.start();
        thread.join();

        assertThat(awaitCompleted).isTrue();
    }

    @Test
    void subsequentAwaitDoesNotBlock() throws Exception {
        latch.await();
        latch.await();
    }

    @Test
    void subsequentAwaitDoesNotBlockEvenIfLatchFileIsDeleted() throws Exception {
        latch.await();

        latchFile.delete();
        latch.await();
    }

    @Test
    void doesNotBlockIfFileExistsPriorToCreatingLatch() throws Exception {

        final FileBasedOneShotLatch latch = new FileBasedOneShotLatch(latchFile.toPath());
        latch.await();
    }
}
