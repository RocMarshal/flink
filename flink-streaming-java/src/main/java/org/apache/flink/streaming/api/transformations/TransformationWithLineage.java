/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.lineage.LineageVertex;

/**
 * A {@link Transformation} that contains lineage information.
 *
 * @param <T> The type of the elements that result from this {@code Transformation}
 * @see Transformation
 */
@Internal
public abstract class TransformationWithLineage<T> extends PhysicalTransformation<T> {
    private LineageVertex lineageVertex;

    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param outputType The output type of this {@code Transformation}
     * @param parallelism The parallelism of this {@code Transformation}
     */
    TransformationWithLineage(String name, TypeInformation<T> outputType, int parallelism) {
        super(name, outputType, parallelism);
    }

    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param outputType The output type of this {@code Transformation}
     * @param parallelism The parallelism of this {@code Transformation}
     * @param parallelismConfigured If true, the parallelism of the transformation is explicitly set
     *     and should be respected. Otherwise the parallelism can be changed at runtime.
     */
    TransformationWithLineage(
            String name,
            TypeInformation<T> outputType,
            int parallelism,
            boolean parallelismConfigured) {
        super(name, outputType, parallelism, parallelismConfigured);
    }

    /** Returns the lineage vertex of this {@code Transformation}. */
    public LineageVertex getLineageVertex() {
        return lineageVertex;
    }

    /** Change the lineage vertex of this {@code Transformation}. */
    public void setLineageVertex(LineageVertex lineageVertex) {
        this.lineageVertex = lineageVertex;
    }
}
