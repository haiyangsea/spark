package org.apache.spark.shuffle.coflow

import org.apache.spark.shuffle._
import org.apache.spark.{Logging, TaskContext, ShuffleDependency, SparkConf}
import org.apache.spark.storage.CoflowManager
import varys.framework.CoflowType
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

/**
 * Created by hWX221863 on 2014/9/22.
 */
class CoflowShuffleManager(
         conf: SparkConf,
         baseShuffleManager: ShuffleManager,
         coflowManager: CoflowManager)
  extends ShuffleManager with Logging {

  private var coflowId: String = null

  /* Register a shuffle with the manager and obtain a handle for it to pass to tasks. */
  override def registerShuffle[K, V, C](
       shuffleId: Int,
       numMaps: Int,
       dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {

    val coflowName: String = "ShuffleCoflow-" + shuffleId
    val maxFlows: Int = dependency.partitioner.numPartitions * numMaps
    coflowId = coflowManager.registerCoflow(coflowName, maxFlows, CoflowType.SHUFFLE)
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
    val futures = Future.traverse(0 until shuffleHandle.numMaps)(mapId => Future {
      coflowManager.waitBlockReady(mapId + "-" + startPartition)
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
      coflowManager.putBlock(mapId + "-" + reduceId, CoflowShuffleManager.BLOCK_SIZE, 1)
    })

    baseShuffleManager.getWriter(handle, mapId, context)
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    coflowManager.unregisterCoflow()
    baseShuffleManager.unregisterShuffle(shuffleId)
  }

  override def shuffleBlockManager: ShuffleBlockManager = {
    baseShuffleManager.shuffleBlockManager
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    baseShuffleManager.stop()
  }
}

object CoflowShuffleManager {
  val BLOCK_SIZE: Long = 0
}
