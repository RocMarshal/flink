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

package org.apache.flink.runtime.slots;

import org.apache.flink.runtime.clusterframework.types.LoadableResourceProfile;
import org.apache.flink.runtime.util.ResourceCounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Default implementation of {@link RequirementMatcher}. This matcher finds the first requirement
 * that a) is not unfulfilled and B) matches the resource profile.
 */
public class DefaultRequirementMatcher implements RequirementMatcher {

    public static final Logger LOG = LoggerFactory.getLogger(DefaultRequirementMatcher.class);

    @Override
    public Optional<LoadableResourceProfile> match(
            LoadableResourceProfile resourceProfileTryAccepted,
            ResourceCounter totalRequirements,
            Function<LoadableResourceProfile, Integer> numAssignedResourcesLookup) {
        // Short-cut for fine-grained resource management. If there is already exactly equal
        // requirement, we can directly match with it.
        if (totalRequirements.getLoadableResourceCount(resourceProfileTryAccepted)
                > numAssignedResourcesLookup.apply(resourceProfileTryAccepted)) {
            LOG.info(
                    "__debug: resourceProfile {}, match result: {}",
                    resourceProfileTryAccepted,
                    resourceProfileTryAccepted);
            return Optional.of(resourceProfileTryAccepted);
        }

        for (Map.Entry<LoadableResourceProfile, Integer> requirementCandidate :
                totalRequirements.getLoadableResourcesWithCount()) {
            LoadableResourceProfile requirementLoadableProfile = requirementCandidate.getKey();

            // beware the order when matching resources to requirements, because
            // ResourceProfile.UNKNOWN (which only
            // occurs as a requirement) does not match any resource!
            if (resourceProfileTryAccepted.isMatching(requirementLoadableProfile)
                    && requirementCandidate.getValue()
                            > numAssignedResourcesLookup.apply(resourceProfileTryAccepted)) {
                LOG.info(
                        "__debug: resourceProfile {}, match result: {}",
                        resourceProfileTryAccepted,
                        requirementLoadableProfile);

                return Optional.of(requirementLoadableProfile);
            }
        }
        LOG.info("__debug: resourceProfile {}, match result: null", resourceProfileTryAccepted);

        return Optional.empty();
    }
}
