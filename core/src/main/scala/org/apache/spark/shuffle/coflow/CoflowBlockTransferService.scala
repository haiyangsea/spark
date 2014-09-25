package org.apache.spark.shuffle.coflow

import org.apache.spark.network._
import org.apache.spark.storage.StorageLevel
import scala.concurrent.{ExecutionContext, Future}
import java.nio.ByteBuffer
import org.apache.spark.util.Utils

/**
 * Created by hWX221863 on 2014/9/25.
 */
class CoflowBlockTransferService(
        coflowManager: CoflowManager,
        transferHandler: BlockTransferService)
  extends BlockTransferService {

  implicit val futureExecContext = ExecutionContext.fromExecutor(
    Utils.newDaemonCachedThreadPool("Connection manager future execution context"))

  /**
   * Initialize the transfer service by giving it the BlockDataManager that can be used to fetch
   * local blocks or put local blocks.
   */
  override def init(blockDataManager: BlockDataManager) {
    transferHandler.init(blockDataManager)
  }

  /**
   * Tear down the transfer service.
   */
  override def stop() {
    transferHandler.stop()
  }

  /**
   * Port number the service is listening on, available only after [[init]] is invoked.
   */
  override def port: Int = {
    transferHandler.port
  }

  /**
   * Host name the service is listening on, available only after [[init]] is invoked.
   */
  override def hostName: String = {
    transferHandler.hostName
  }

  /**
   * Fetch a sequence of blocks from a remote node asynchronously,
   * available only after [[init]] is invoked.
   *
   * Note that [[BlockFetchingListener.onBlockFetchSuccess]] is called once per block,
   * while [[BlockFetchingListener.onBlockFetchFailure]] is called once per failure (not per block).
   *
   * Note that this API takes a sequence so the implementation can batch requests, and does not
   * return a future so the underlying implementation can invoke onBlockFetchSuccess as soon as
   * the data of a block is fetched, rather than waiting for all blocks to be fetched.
   */
  override def fetchBlocks(
     hostName: String,
     port: Int,
     blockIds: Seq[String],
     listener: BlockFetchingListener) {

     val futures = Future.traverse(blockIds)(blockId => Future {
       val data: Array[Byte] = coflowManager.getBlock(blockId)
       listener.onBlockFetchSuccess(
         blockId, new NioByteBufferManagedBuffer(ByteBuffer.wrap(data)))
     })

     futures.onFailure({ case exception =>
        listener.onBlockFetchFailure(exception)
     })(futureExecContext)
  }

  /**
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   */
  override def uploadBlock(
     hostname: String,
     port: Int,
     blockId: String,
     blockData: ManagedBuffer,
     level: StorageLevel): Future[Unit] = {
     transferHandler.uploadBlock(hostname, port, blockId, blockData, level)
  }
}
