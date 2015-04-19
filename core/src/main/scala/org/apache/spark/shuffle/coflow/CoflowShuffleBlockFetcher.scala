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

package org.apache.spark.shuffle.coflow

import org.apache.spark.util.CompletionIterator
import org.apache.spark.{Logging, SparkEnv, TaskContext, InterruptibleIterator}
import org.apache.spark.serializer.Serializer

/**
 * Created by hWX221863 on 2014/9/26.
 */
object CoflowShuffleBlockFetcher extends Logging {
  def fetch[T](
      context: TaskContext,
      shuffleId: Int,
      reduceId: Int,
      numMaps: Int,
      serializer: Serializer,
      coflowManager: CoflowManager)
  : Iterator[T] = {
    logDebug("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    val blockManager = SparkEnv.get.blockManager

    val startTime = System.currentTimeMillis
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId)
    logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
      shuffleId, reduceId, System.currentTimeMillis - startTime))

    val blockMapIdsAndSize: Array[(Long, Int)] = statuses.map(status => status._2)
      .zipWithIndex

    val blocksIterator = new CoflowBlockShuffleIterator(context,
      shuffleId,
      reduceId,
      blockMapIdsAndSize,
      serializer,
      coflowManager,
      blockManager)
    val itr = blocksIterator.flatMap(_.asInstanceOf[Iterator[T]])

    val completionIter = CompletionIterator[T, Iterator[T]](itr, {
      context.taskMetrics.updateShuffleReadMetrics()
    })

    new InterruptibleIterator[T](context, completionIter)
  }
}
