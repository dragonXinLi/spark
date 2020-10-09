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

/**
 * An identifier for a partition in an RDD.
 */
trait Partition extends Serializable {
  // RDD只是数据集的抽象，分区内部并不会存储具体的数据。Partition类内包含一个index成员。表示该分区在RDD内的编号，通过RDD编号+分区
  // 编号可以唯一确定该分区对应的块编号，利用底层数据存储层提供的接口，就能从存储介质（HDFS，Memory）中提取出分区对应的数据
  /**
   * Get the partition's index within its parent RDD
   */
  def index: Int

  // A better default implementation of HashCode
  override def hashCode(): Int = index

  override def equals(other: Any): Boolean = super.equals(other)
}
