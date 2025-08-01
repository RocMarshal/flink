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

package org.apache.flink.runtime.rest.handler.job.rescales;

import org.apache.flink.runtime.scheduler.adaptive.timeline.Rescale;

import org.apache.flink.shaded.guava33.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava33.com.google.common.cache.CacheBuilder;

import javax.annotation.Nullable;

/** A size-based cache of accessed rescales for completed, failed and ignored rescales. */
public class RescaleCache {

    @Nullable private final Cache<Long, Rescale> cache;

    public RescaleCache(int maxNumEntries) {
        if (maxNumEntries > 0) {
            this.cache = CacheBuilder.newBuilder().maximumSize(maxNumEntries).build();
        } else {
            this.cache = null;
        }
    }

    /**
     * Try to add the rescale to the cache.
     *
     * @param rescale job rescale statistics to be added.
     */
    public void tryAdd(Rescale rescale) {
        // Don't add in progress rescales as they will be replaced by their
        // completed/failed version eventually.
        if (cache != null
                && rescale != null
                && rescale.getStatus() != null
                && rescale.getStatus().isSealed()) {
            cache.put(rescale.getIdEpoch().getRescaleId(), rescale);
        }
    }

    /**
     * Try to look up a rescale by it's ID in the cache.
     *
     * @param rescaleId ID of the rescale to look up.
     * @return The job rescale or <code>null</code> if rescale not found.
     */
    public Rescale tryGet(long rescaleId) {
        if (cache != null) {
            return cache.getIfPresent(rescaleId);
        } else {
            return null;
        }
    }
}
