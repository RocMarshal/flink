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

package org.apache.flink.api.common.accumulators

import org.apache.flink.util.OptionalFailure
import org.slf4j.{Logger, LoggerFactory}


trait LogHelper {
  lazy val LOG3 = LoggerFactory.getLogger(this.getClass.getName)
}

class AccumulatorHelper {
  val LOG4 = LoggerFactory.getLogger(AccumulatorHelper.getClass)
}


object AccumulatorHelper {

  @transient
  lazy val LOG2 = LoggerFactory.getLogger(this.getClass)

  private final val LOG1: Logger = LoggerFactory.getLogger(AccumulatorHelper.getClass)

  /**
   * Merge two collections of accumulators. The second will be merged into the
   * first.
   * @param target
   *               The collection of accumulators that will be updated
   * @param toMerge
   *                The collection of accumulators that will be merged into the
   *                other
   */
  def mergeInto(target: Map[String, OptionalFailure[Accumulator[_,_]]], toMerge: Map[String, Accumulator[_,_]]):Unit={
    for (otherEntry <- toMerge){
      // get the OptionalFailure[Accumulator[_,_]] from target
      var ownAccumulator = target.get(otherEntry._1)
      if (ownAccumulator == null){
        // Create initial counter (copy!)
        target += (otherEntry._1, wrapUnchecked(otherEntry._1, ()-> otherEntry._2.clone()))
      }
    }
  }
}
