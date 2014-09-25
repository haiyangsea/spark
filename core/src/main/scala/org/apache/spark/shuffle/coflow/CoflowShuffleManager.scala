package org.apache.spark.shuffle.coflow

import org.apache.spark.shuffle._
import org.apache.spark.{Logging, TaskContext, ShuffleDependency, SparkConf}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import varys.framework.CoflowType
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by hWX221863 on 2014/9/22.
 */
class CoflowShuffleManager(
         conf: SparkConf,
         baseShuffleManager: ShuffleManager,
         coflowManager: CoflowManager)
  extends ShuffleManager with Logging {

  /* Register a shuffle with the manager and obtain a handle for it to pass to tasks. */
  override def registerShuffle[K, V, C](
       shuffleId: Int,
       numMaps: Int,
       dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {

    val coflowName: String = CoflowManager.makeCoflowName(shuffleId, conf)
    val maxFlows: Int = dependency.partitioner.numPartitions * numMaps
    val coflowId = coflowManager.registerCoflow(shuffleId, coflowName, maxFlows, CoflowType.SHUFFLE)
    logInfo("registered coflow,get id " + coflowId)

    baseShuffleManager.registerShuffle(shuffleId, numMaps, dependency)
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    val shuffleHandle: BaseShuffleHandle[K, _, C] =
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]]
    // 等待Coflow调度
    val futures = Future.traverse(Range(0, shuffleHandle.numMaps).toIterator)(mapId => Future {
      val blockId = CoflowManager.makeBlockId(mapId, startPartition)
      logDebug(s"wait block[$blockId] ready...")
      coflowManager.waitBlockReady(handle.shuffleId, blockId)
    })
    Await.result(futures, Duration.Inf)

    baseShuffleManager.getReader(handle, startPartition, endPartition, context)
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext)
      : ShuffleWriter[K, V] = {
    val shuffleHandle: BaseShuffleHandle[K, V, _] =
      handle.asInstanceOf[BaseShuffleHandle[K, V, _]]
    // 在Coflow中注册Map/Reduce结果
    (0 until shuffleHandle.dependency.partitioner.numPartitions).foreach(reduceId => {
      val blockId = CoflowManager.makeBlockId(mapId, reduceId)
      coflowManager.putBlock(handle.shuffleId, blockId, CoflowManager.BLOCK_SIZE, 1)
    })

    baseShuffleManager.getWriter(handle, mapId, context)
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    coflowManager.unregisterCoflow(shuffleId)
    baseShuffleManager.unregisterShuffle(shuffleId)
  }

  override def shuffleBlockManager: ShuffleBlockManager = {
    baseShuffleManager.shuffleBlockManager
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    coflowManager.stop()
    baseShuffleManager.stop()
  }
}
