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

import java.io.IOException
import collection.JavaConversions._
import java.util.function.Supplier
import org.apache.flink.util.function.CheckedSupplier
import org.apache.flink.util.{FlinkException, OptionalFailure, SerializedValue}
import org.slf4j.{Logger, LoggerFactory}
import java.util.{Collections, HashMap => JHashMap, Map => JMap}

import scala.collection.{GenTraversableOnce, mutable}


//trait LogHelper {
//  lazy val LOG3 = LoggerFactory.getLogger(this.getClass.getName)
//}

class AccumulatorHelper {
  val LOG4: Logger = LoggerFactory.getLogger(AccumulatorHelper.getClass)
}

/**
 * Helper functions for the interaction with [[Accumulator]].
 */
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
  def mergeInto(target: JMap[String, OptionalFailure[Accumulator[_,_]]], toMerge: JMap[String, Accumulator[_,_]]):Unit={
    for (otherEntry <- toMerge) {
      // get the OptionalFailure[Accumulator[_,_]] from target
      var ownAccumulator: OptionalFailure[Accumulator[_, _]] = target.getOrElse(otherEntry._1, null)
      if (ownAccumulator == null) {
        // Create initial counter (copy!)
        target += (otherEntry._1->wrapUnchecked(otherEntry._1, new Supplier[Accumulator[_,_]]() {
          override def get(): Accumulator[_, _] = {
            var r1 = otherEntry._2.clone()
            r1
          }
        }))
      } else if (ownAccumulator.isFailure) {
        // do nothing.
      } else {
        var accumulator  = ownAccumulator.getUnchecked
        // Both should have the same type
        compareAccumulatorTypes(otherEntry._1,
          accumulator.getClass.asInstanceOf[Class[Accumulator[_, _ <: java.io.Serializable]]],
          otherEntry._2.getClass.asInstanceOf[Class[Accumulator[_, _ <: java.io.Serializable]]])
        // Merge target counter with other counter
        target += (otherEntry._1->wrapUnchecked(otherEntry._1,
          new Supplier[Accumulator[_, _]] {
            override def get(): Accumulator[_, _] = {
              mergeSingle(accumulator, otherEntry._2.clone())
            }
          }))
      }
    }
  }

  /**
   * Workaround method for type safety.
   */
  private def mergeSingle[V, R <: java.io.Serializable] (target: Accumulator[_, _], toMerge: Accumulator[_,_]) ={
    val typedTarget = target.asInstanceOf[Accumulator[V, R]]
    val typedToMerge = toMerge.asInstanceOf[Accumulator[V, R]]
    typedTarget.merge(typedToMerge)
    typedTarget
  }

  /**
   * Compare both classes and throw [[UnsupportedOperationException]] if they differ.
   *
   */
  @throws(classOf[UnsupportedOperationException])
  @SuppressWarnings(Array("rawtypes"))
  def compareAccumulatorTypes(name: Any,
                              first: Class[_ <: Accumulator[_, _ <: java.io.Serializable]],
                              second: Class[_ <: Accumulator[_, _ <: java.io.Serializable]]): Unit={
    if (first == null || second == null) {
      throw new NullPointerException
    }

    if (first != second) {
      if (!first.getName.equals(second.getName)){
        throw new UnsupportedOperationException("The accumulator object '" + name +
          "' was created with two different types: " + first.getName + " and " + second.getName)
      } else {
        // damn, name is the same, but different classloaders
        throw new UnsupportedOperationException("The accumulator object '" + name
          + "' was created with two different classes: " + first + " and " + second
          + " Both have the same type (" + first.getName + ") but different classloaders: "
          + first.getClassLoader + " and " + second.getClassLoader)
      }
    }
  }

  def toResultMap(accumulators: JMap[String, Accumulator[_,_]]): JMap[String, OptionalFailure[Any]] = {
    val resultMap = new JHashMap[String, OptionalFailure[Any]]()
    for (entry <- accumulators) {
      resultMap += (entry._1->
                    wrapUnchecked(entry._1, new Supplier[Any]() {
                                                  override def get(): Any = {
                                                    val result = entry._2.getLocalValue
                                                    result.asInstanceOf[Any]
                                                  }
                                            }
                                  )
                    )
    }
    resultMap
  }

  private def wrapUnchecked[R](name: String, supplier: Supplier[R]): OptionalFailure[R] = {
    OptionalFailure.createFrom(new CheckedSupplier[R] {
      override def get(): R = {
        try {
          supplier.get()
        }catch{case ex:RuntimeException =>{
          LOG2.error("Unexpected error while handling accumulator [" + name + "]", ex);
          throw new FlinkException(ex);
        }}
      }
    })
  }

  def getResultsFormatted(map: JMap[String, Any]): String = {
    var builder = new mutable.StringBuilder()
    for (entry<- map){
      builder
      .append("- ")
      .append(entry._1)
      .append(" (")
      .append(entry._2.getClass.getName)
      .append(")")
      map.size
      if (entry._2.isInstanceOf[GenTraversableOnce[_]]) {
        builder.append(" [").append((entry._2.asInstanceOf[GenTraversableOnce[_]]).size).append(" elements]");
      } else {
        builder.append(": ").append(entry._2.toString)
      }
      builder.append(System.lineSeparator)
    }
    builder.toString()
  }

  def copy(accumulators: JMap[String, Accumulator[_, _ <: java.io.Serializable]]) = {
    val result = new JHashMap[String, Accumulator[_, _]]()
    for (entry<- accumulators){
      result += (entry._1->entry._2.clone())
    }
    result
  }


  /**
   * Takes the serialized accumulator results and tries to deserialize them using the provided
   * class loader.
   *
   * @param serializedAccumulators The serialized accumulator results.
   * @param loader                 The class loader to use.
   * @return The deserialized accumulator results.
   */
  @throws(classOf[IOException])
  @throws(classOf[ClassNotFoundException])
  def deserializeAccumulators(serializedAccumulators: JMap[String, SerializedValue[OptionalFailure[Any
  ]]], loader: ClassLoader): JMap[String, OptionalFailure[Any]]= {
    if (serializedAccumulators == null || serializedAccumulators.isEmpty) return new JHashMap[String, OptionalFailure[Any]]()
    var accumulators = new JHashMap[String, OptionalFailure[Any]]()
    for (entry<- serializedAccumulators){
      var value : OptionalFailure[Any] = null
      if (entry._2 != null){
        value = entry._2.deserializeValue(loader)
      }
      accumulators += (entry._1->value)
    }
    accumulators
  }

  /**
   * Takes the serialized accumulator results and tries to deserialize them using the provided
   * class loader, and then try to unwrap the value unchecked.
   *
   * @param serializedAccumulators The serialized accumulator results.
   * @param loader                 The class loader to use.
   * @return The deserialized and unwrapped accumulator results.
   */
  @throws(classOf[IOException])
  @throws(classOf[ClassNotFoundException])
  def deserializeAndUnwrapAccumulators(serializedAccumulators: JMap[String, SerializedValue[OptionalFailure[Any]]],
                                       loader: ClassLoader): JMap[String, Any]={
    var deserializedAccumulators = deserializeAccumulators(serializedAccumulators,loader)
    if (deserializedAccumulators.isEmpty) {
      new JHashMap[String, Any]()
    }
    var accumulators = new JHashMap[String, Any]()
    for (entry<- deserializedAccumulators) {
      accumulators += (entry._1->entry._2.getUnchecked)
    }
    accumulators
  }

}
