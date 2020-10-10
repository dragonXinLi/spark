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

package org.apache.spark

import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleHandle

/**
 * :: DeveloperApi ::
 * Base class for dependencies.
 */
/*
RDD的容错机制是通过记录更新来实现的，且记录的是粗粒度的转换操作。在外部，我们将记录的信息称为血统关系。
而到了源码级别，spark记录的则是RDD之间的依赖关系。在一次转化操作中，创建得到的新RDD称为子RDD，提供数据的RDD称为父RDD，
父RDD可能会存在多个，我们把子RDD与父RDD之间的关系称为依赖关系或者可以说是子RDD依赖于父RDD。

依赖只保存父RDD信息。转化操作的其他信息，如数据处理函数，会在创建新RDD时候，保存在新的RDD内。

每个Dependency子类内部都会存储一个RDD对象，对应一个父RDD，如果一次转换操作有多个父RDD，就会对应产生多个Dependency对象，
所有的Dependency对象存储在子RDD内部，通过遍历RDD内部的Dependency对象，就能获取该RDD所有依赖的父RDD
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}


/**
 * :: DeveloperApi ::
 * Base class for dependencies where each partition of the child RDD depends on a small number
 * of partitions of the parent RDD. Narrow dependencies allow for pipelined execution.
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * Get the parent partitions for a child partition.
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
   */
  /*
  NarrowDependency要求子类实现getParents方法，用于获取一个分区数据来源于父RDD中的哪些分区，
  虽然要求返回Seq[Int]，实际上却只有一个元素
   */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd
}


/**
 * :: DeveloperApi ::
 * Represents a dependency on the output of a shuffle stage. Note that in the case of shuffle,
 * the RDD is transient since we don't need it on the executor side.
 *
 * @param _rdd the parent RDD
 * @param partitioner partitioner used to partition the shuffle output
 * @param serializer [[org.apache.spark.serializer.Serializer Serializer]] to use. If not set
 *                   explicitly then the default serializer, as specified by `spark.serializer`
 *                   config option, will be used.
 * @param keyOrdering key ordering for RDD's shuffles
 * @param aggregator map/reduce-side aggregator for RDD's shuffle
 * @param mapSideCombine whether to perform partial aggregation (also known as map-side combine)
 */
/*
rdd:用于表示Shuffle依赖中，子RDD所依赖的父RDD
shuffleId:shuffle的ID编号，在一个spark应用程序中，每个shuffle的编号都是唯一的
shuffleHandle:shuffle句柄，shuffleHandle内部一般包含shuffleID,Mapper的个数以及对应的shuffle依赖，在执行shuffleMapTask的时候，
任务可以用个shuffleManager获取该句柄，并进一步得到shuffle相关信息
partitioner:分区器，用于决定shuffle过程中Reducer的个数，（实际上是子RDD的分区个数）以及Map端的一条数据记录应该分配给哪一个Reducer,
也可以被用在COGroupRDD中，确定父RDD与子RDD之间的依赖关系类型
serializer:序列化器，用于shuffle过程中Map段数据的序列化和Reduce段数据的反序列化
KeyOrdering:键值排序策略，用于决定子RDD的一个分区内，如何根据键值对类型数据记录进行排序
Aggregator:聚合器，内部包含了多个聚合函数.例如对于groupbykey操作，createCombiners表示把第一个元素放入到集合中，
mergeValue表示一个元素添加到集合中，mergeCombiners表示把两个集合进行合并。这些函数被用于shuffle过程中数据的聚合。
mapSideCombine:用于指定shuffle过程中是否需要在map端进行combine操作。如果指定该值为true,由于combine操作需要用到聚合器中的
相关聚合函数，因此Aggregator不能为空，否则spark会抛出异常。如groupbykey转换操作对应的shuffleDependency中，mapSideCombine=false,
而reduceByKey转换操作中，mapSideCombine=true
 */
@DeveloperApi
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Serializer = SparkEnv.get.serializer,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false)
  extends Dependency[Product2[K, V]] {

  if (mapSideCombine) {
    require(aggregator.isDefined, "Map-side combine without Aggregator specified!")
  }
  override def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  private[spark] val keyClassName: String = reflect.classTag[K].runtimeClass.getName
  private[spark] val valueClassName: String = reflect.classTag[V].runtimeClass.getName
  // Note: It's possible that the combiner class tag is null, if the combineByKey
  // methods in PairRDDFunctions are used instead of combineByKeyWithClassTag.
  private[spark] val combinerClassName: Option[String] =
    Option(reflect.classTag[C]).map(_.runtimeClass.getName)

  val shuffleId: Int = _rdd.context.newShuffleId()

  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, _rdd.partitions.length, this)

  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between partitions of the parent and child RDDs.
 */
/*
一对一依赖表示子RDD分区的编号于父RDD分区的编号完全一致的情况，若两个RDD之间存在着一对一依赖，则子RDD的分区个数，
分区内记录的个数都将继承自父RDD
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
 * @param rdd the parent RDD
 * @param inStart the start of the range in the parent RDD
 * @param outStart the start of the range in the child RDD
 * @param length the length of the range
 */
/*
范围依赖是依赖关系中的一个特例，只被用于表示unionRDD与父RDD之间的依赖关系。
想比一对一依赖，除了第一个父RDD，其他父RDD和子RDD的分区编号不再一致，spark统一将unionRDD与父RDD之间（包含第一个RDD）
的关系都叫做范围依赖
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): List[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}
