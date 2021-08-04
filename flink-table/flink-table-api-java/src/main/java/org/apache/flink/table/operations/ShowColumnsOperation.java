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

package org.apache.flink.table.operations;

import org.apache.flink.table.catalog.ObjectIdentifier;

/** Show columns from [[catalog.]database.]table. */
public class ShowColumnsOperation implements ShowOperation {

    private ObjectIdentifier tableIdentifier;
    private boolean useLike;
    private String likePattern;
    private String preposition;

    public ShowColumnsOperation(
            ObjectIdentifier tableIdentifier,
            String likePattern,
            boolean useLike,
            String preposition) {
        this.tableIdentifier = tableIdentifier;
        this.likePattern = likePattern;
        this.useLike = useLike;
        this.preposition = preposition;
    }

    public String getLikePattern() {
        return likePattern;
    }

    public String getPreposition() {
        return preposition;
    }

    public void setPreposition(String preposition) {
        this.preposition = preposition;
    }

    public boolean isUseLike() {
        return useLike;
    }

    public void setUseLike(boolean useLike) {
        this.useLike = useLike;
    }

    public void setLikePattern(String likePattern) {
        this.likePattern = likePattern;
    }

    public ObjectIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public void setTableIdentifier(ObjectIdentifier tableIdentifier) {
        this.tableIdentifier = tableIdentifier;
    }

    @Override
    public String asSummaryString() {
        if (useLike) {
            return String.format(
                    "SHOW COLUMNS %s %s LIKE '%s'",
                    preposition, tableIdentifier.asSummaryString(), likePattern);
        }
        return String.format("SHOW COLUMNS %s %s", preposition, tableIdentifier.asSummaryString());
    }
}
