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

import org.apache.spark.{Logging, TaskContext}
import org.apache.spark.storage.{ShuffleBlockId, BlockManager}
import java.util.concurrent.{Executors, LinkedBlockingQueue}
import org.apache.spark.network.NioByteBufferManagedBuffer
import java.nio.ByteBuffer
import org.apache.spark.serializer.Serializer
import varys.framework.network.FlowFetchListener

/**
 * Created by hWX221863 on 2014/9/26.
 */
private[spark] class CoflowBlockShuffleIterator(
    val context: TaskContext,
    shuffleId: Int,
    reduceId: Int,
    blockMapIdsAndSize: Array[(Long, Int)],
    serializer: Serializer,
    coflowManager: CoflowManager,
    blockManager: BlockManager)
  extends Iterator[Iterator[Any]] with Logging {

  private[this] var numBlocksProcessed = 0
  private[this] val numBlocksToFetch = blockMapIdsAndSize.count(block => block._1 > 0)

  private[this] val blocks = new LinkedBlockingQueue[Iterator[Any]]
  private[this] val shuffleMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()

  initialize()
  // TODO add metrics
  private[this] def initialize() {
    val flowIds = blockMapIdsAndSize.map {
      case (size, mapId) => CoflowManager.makeFileId(shuffleId, mapId, reduceId)
    }

    coflowManager.getFlows(shuffleId, flowIds, new FlowFetchListener {
      override def onFlowFetchFailure(coflowId: String,
                                      flowId: String,
                                      length: Long,
                                      exception: Throwable): Unit = {
        logWarning(s"Failed fetch flow with id $flowId in coflow $coflowId", exception)
      }

      override def onFlowFetchSuccess(coflowId: String,
                                      flowId: String, dataBuffer: ByteBuffer): Unit = {

        val mapId = CoflowManager.getBlockId(flowId).mapId
        logDebug(s"get block[shuffle id = $shuffleId, " +
          s"map id = $mapId, reduce id = $reduceId] data.")
        val managedBuffer = new NioByteBufferManagedBuffer(dataBuffer)
        if (managedBuffer.size > 0) {
          val blockIterator = serializer.newInstance().deserializeStream(
            blockManager.wrapForCompression(ShuffleBlockId(shuffleId, mapId, reduceId),
              managedBuffer.inputStream())).asIterator
          blocks.put(blockIterator)
          shuffleMetrics.remoteBlocksFetched += 1
          shuffleMetrics.remoteBytesRead += managedBuffer.size
        }
      }
    })

  }

  def hasNext: Boolean = {
      numBlocksProcessed < numBlocksToFetch
  }

  def next(): Iterator[Any] = {
    numBlocksProcessed += 1
    logInfo("fetch shuffle[shuffle id = %d, reduce id = %d] block %d time(s), total %d."
      .format(shuffleId, reduceId, numBlocksProcessed, numBlocksToFetch))

    val startFetchWait = System.currentTimeMillis()
    val block = blocks.take()
    val stopFetchWait = System.currentTimeMillis()
    shuffleMetrics.fetchWaitTime += (stopFetchWait - startFetchWait)
    block
  }
}