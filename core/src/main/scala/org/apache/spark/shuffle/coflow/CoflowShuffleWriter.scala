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
    val reduceCount: Int = handle.dependency.partitioner.numPartitions
    // 在Coflow中注册Map/Reduce结果
    (0 until reduceCount).foreach(reduceId => {
      val blockId = ShuffleBlockId(handle.shuffleId, mapId, reduceId)
      val fileSegment: FileSegment = getFileSegment(blockId: ShuffleBlockId)
      coflowManager.putFile(handle.shuffleId, blockId.name, fileSegment, reduceCount)
    })
    shuffleWriterHandler.stop(success)
  }
}
