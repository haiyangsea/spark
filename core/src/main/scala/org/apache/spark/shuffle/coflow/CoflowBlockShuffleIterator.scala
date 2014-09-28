package org.apache.spark.shuffle.coflow

import org.apache.spark.{Logging, TaskKilledException, TaskContext}
import org.apache.spark.storage.{ShuffleBlockId, BlockId, BlockManager}
import java.util.concurrent.{ThreadFactory, Executors, ThreadPoolExecutor, LinkedBlockingQueue}
import org.apache.spark.network.{NioByteBufferManagedBuffer, ManagedBuffer}
import scala.concurrent.{ExecutionContext, Future}
import java.nio.ByteBuffer
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils
import java.util.concurrent.atomic.AtomicInteger
import com.google.common.util.concurrent.ThreadFactoryBuilder

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

  implicit val futureExecFuture =
    ExecutionContext.fromExecutor(CoflowBlockShuffleIterator.newDaemonCachedThreadPool())

  initialize()

  private[this] def initialize() {
    blockMapIdsAndSize.map(block => block._2).foreach(mapId => {
      Future {
        val dataBuffer: ByteBuffer = fetchBlock(mapId)
        if(dataBuffer.array().length > 0) {
          val managedBuffer = new NioByteBufferManagedBuffer(dataBuffer)
          val blockIterator = serializer.newInstance().deserializeStream(
            blockManager.wrapForCompression(ShuffleBlockId(shuffleId, mapId, reduceId),
              managedBuffer.inputStream())).asIterator
          blocks.put(blockIterator)
        }
      }
    })
  }

  private[this] def fetchBlock(mapId: Int): ByteBuffer = {
    val fileId: String = CoflowManager.makeFileId(shuffleId, mapId, reduceId)
    logInfo(s"start to fetch block[$fileId] data through coflow")
    val data = coflowManager.getFile(shuffleId, fileId)
    ByteBuffer.wrap(data)
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

object CoflowBlockShuffleIterator {
  // TODO 检查用Spark Util中生成的futureContext为何会容易达到瓶颈
  private[this] val daemonThreadFactory: ThreadFactory =
    new ThreadFactoryBuilder().setDaemon(true).build()

  def newDaemonCachedThreadPool(): ThreadPoolExecutor =
    Executors.newCachedThreadPool(daemonThreadFactory).asInstanceOf[ThreadPoolExecutor]
}
