package org.apache.spark.shuffle.coflow

import org.apache.spark.util.CompletionIterator
import org.apache.spark.{SparkEnv, TaskContext, InterruptibleIterator}
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.serializer.Serializer

/**
 * Created by hWX221863 on 2014/9/26.
 */
object CoflowShuffleBlockFetcher {
  def fetch[T](
      context: TaskContext,
      shuffleId: Int,
      reduceId: Int,
      numMaps: Int,
      serializer: Serializer,
      coflowManager: CoflowManager)
  : Iterator[T] = {
    val blockManager = SparkEnv.get.blockManager
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId)
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
