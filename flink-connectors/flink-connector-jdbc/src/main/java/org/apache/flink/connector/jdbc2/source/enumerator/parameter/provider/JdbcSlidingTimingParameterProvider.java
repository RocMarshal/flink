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

package org.apache.flink.connector.jdbc2.source.enumerator.parameter.provider;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class JdbcSlidingTimingParameterProvider implements JdbcParameterValuesProvider {

    private final long slideStepMills;
    private final long slideSpanMills;

    private Long startMills;

    public JdbcSlidingTimingParameterProvider(
            Long startMills, long slideSpanMills, long slideStepMills) {
        Preconditions.checkArgument(startMills != null && startMills > 0L);
        Preconditions.checkArgument(
                slideSpanMills <= 0 || slideStepMills <= 0,
                "JdbcSlidingTimingParameterProvider parameters must satisfy "
                        + "slideSpanMills > 0 and slideStepMills > 0");
        this.startMills = startMills;
        this.slideStepMills = slideStepMills;
        this.slideSpanMills = slideSpanMills;
    }

    private boolean shouldEnumerateNextSplit(Long timeMills) {
        return latestEndMills() > timeMills && (latestEndMills() - timeMills >= slideSpanMills);
    }

    @Override
    public Long getLatestOptionalState() {
        return startMills;
    }

    @Override
    public void setOptionalState(Serializable optionalState) {
        Preconditions.checkArgument((Long) optionalState > 0L);
        this.startMills = (Long) optionalState;
    }

    public Long latestEndMills() {
        return System.currentTimeMillis();
    }

    @Override
    public Serializable[][] getParameterValues() {
        List<Serializable[]> tmpList = new ArrayList<>();
        while (shouldEnumerateNextSplit(startMills)) {
            Serializable[] params = new Serializable[] {startMills, startMills + slideSpanMills};
            tmpList.add(params);
            startMills += slideStepMills;
        }
        return tmpList.toArray(new Serializable[0][]);
    }
}
