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

import org.apache.spark.shuffle._
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.TaskContext
import org.apache.spark.storage.{FileSegment, ShuffleBlockId}
import org.apache.spark.network.FileSegmentManagedBuffer

/**
 * Created by hWX221863 on 2014/9/25.
 */
class CoflowShuffleWriter[K, V](
        mapId: Int,
        handle: BaseShuffleHandle[K, V, _],
        context: TaskContext,
        coflowManager: CoflowManager,
        shuffleManager: ShuffleManager
    ) extends ShuffleWriter[K, V] {
  val shuffleWriterHandler: ShuffleWriter[K, V] = shuffleManager.getWriter(handle, mapId, context)

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[_ <: Product2[K, V]]) {
    shuffleWriterHandler.write(records)
  }

  private def getFileSegment(blockId: ShuffleBlockId): FileSegment = {
    val fileManagerBuffer = shuffleManager.shuffleBlockManager
      .getBlockData(blockId).asInstanceOf[FileSegmentManagedBuffer]
    new FileSegment(fileManagerBuffer.file, fileManagerBuffer.offset, fileManagerBuffer.length)
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    val mapStatus = shuffleWriterHandler.stop(success)
    val reduceCount: Int = handle.dependency.partitioner.numPartitions
    // 在Coflow中注册Map/Reduce结果
    (0 until reduceCount).foreach(reduceId => {
      val blockId = ShuffleBlockId(handle.shuffleId, mapId, reduceId)
      val fileSegment: FileSegment = getFileSegment(blockId)
      val fileId: String = CoflowManager.makeFileId(blockId)
      println(s"flow id $fileId, length = ${fileSegment.length}")
      coflowManager.putFile(handle.shuffleId, fileId, fileSegment, reduceCount)
    })
    mapStatus
  }
}
