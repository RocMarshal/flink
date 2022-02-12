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

package org.apache.flink.connector.jdbc.source.reader;

/**
 * A record, together with the reader position to be stored in the checkpoint.
 *
 * <p>The position defines the point in the reader AFTER the record. Record processing and updating
 * checkpointed state happens atomically.
 *
 * <p>This class is immutable for safety. The {@link #offset} is declared in the integer type
 * because of the type of the parameter defined in {@link java.sql.ResultSet#absolute(int)}.
 */
public class RecordAndOffset<E> {

    final E record;
    final int offset;

    public RecordAndOffset(E record, int offset) {
        this.record = record;
        this.offset = offset;
    }

    // ------------------------------------------------------------------------

    public E getRecord() {
        return record;
    }

    public int getOffset() {
        return offset;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format("%s @ %d", record, offset);
    }
}
