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

package org.apache.spark.shuffle

import org.apache.spark.{ShuffleDependency, TaskContext}

/**
 * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
 * and on each executor, based on the spark.shuffle.manager setting. The driver registers shuffles
 * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
 *
 * NOTE: this will be instantiated by SparkEnv so its constructor can take a SparkConf and
 * boolean isDriver as parameters.
 */
/*
老版本中ShuffleMananger其实有2个实现，
1.HashShuffleManager
2.SortShuffleManager
新版本中，ShuffleManager只有1个实现
1.SortShuffleManager

解决：
1.每个ShuffleMapTask按照什么规则进行write
2.每个ShuffleReduceTask按照什么规则进行Reduce？因为每个reduceTask通过ShuffleID和Reduce，
只能获取一组表Map输出的mapStatus，Reduce怎么从这组mapStatus读取指定Reduce的数据。

HashShuffleManager.scala
def forMapTask(shuffleId: Int, mapId: Int, numBuckets: Int, serializer: Serializer,
      writeMetrics: ShuffleWriteMetrics) = {
    new ShuffleWriterGroup {
      val writers: Array[BlockObjectWriter] ={
        Array.tabulate[BlockObjectWriter](numBuckets) { bucketId =>
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
          val blockFile = blockManager.diskBlockManager.getFile(blockId)
          if (blockFile.exists) {
            if (blockFile.delete()) {
              logInfo(s"Removed existing shuffle file $blockFile")
            } else {
              logWarning(s"Failed to remove existing shuffle file $blockFile")
            }
          }
          blockManager.getDiskWriter(blockId, blockFile, serializer, bufferSize, writeMetrics)
        }
      }
    }
  }

这个函数看起来简单，它针对每个MapTask构造一个ShuffleWriterGroup对象，
该对象包含一组writer,个数为numBuckets，即reduce个数，
这里对文件的管理基于blockManager.diskBlockManager来实现。

这里我们可以看到，对应每个Map都创建numBuckets个小文件，从而证明了ShuffleMapTask个数*reduce个数。


到目前我们应该了解了ShuffleRedr的功能，及两种ShuffleManager的实现，从上面的分析我们可以看到SortShuffleManger
创建的小文件的数目应该是最小的，而且Map输出是有序的，在reduce过程中如果要进行有序合并，代价也是最小的，也因此
SortShuffleManger现在是Spark1.1版本以后的默认配置项；
 */
private[spark] trait ShuffleManager {

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   */
  /*
  向管理器注册Shuffle，并获得一个将其传递给任务的句柄。
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  /** Get a writer for a given partition. Called on executors by map tasks. */
  /*
  为给定的分区获取一个写入器。由mapTask调用执行器
   */
  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V]

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  /*
  为一系列的reduce分区(从开始分区到结束分区-1，包括在内)获取一个读取器。由reduceTask调用执行器
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C]

  /**
   * Remove a shuffle's metadata from the ShuffleManager.
   * @return true if the metadata removed successfully, otherwise false.
   */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
   */
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager. */
  def stop(): Unit
}
