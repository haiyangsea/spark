package org.apache.spark.shuffle.coflow

import akka.actor.ActorRef
import org.apache.spark.{SparkException, SparkConf, Logging}
import varys.framework.client.VarysClient
import org.apache.spark.util.AkkaUtils
import varys.framework.CoflowType.CoflowType
import varys.framework.CoflowDescription
import varys.VarysException
import java.io.File
import scala.concurrent.duration.FiniteDuration
import scala.collection.mutable
import org.apache.spark.shuffle.coflow.CoflowManagerMessages.{RegisteredCoflow, GetCoflow, CoflowInfo}
import org.apache.spark.storage.FileSegment

/**
 * Created by hWX221863 on 2014/9/19.
 */
private[spark]
class CoflowManager(
    coflowDriverActor: ActorRef,
    conf: SparkConf,
    val clientName: String,
    isDriver: Boolean)
  extends Logging {

  private val AKKA_RETRY_ATTEMPTS: Int = AkkaUtils.numRetries(conf)
  private val AKKA_RETRY_INTERVAL_MS: Int = AkkaUtils.retryWaitMs(conf)
  private val timeout: FiniteDuration = AkkaUtils.askTimeout(conf)

  private var varysClient: VarysClient = null;

  var varysMasterUrl: String = null
  var coflowId: String = null

  private val shuffleCoflowPair: mutable.HashMap[Int, String] =
    new mutable.HashMap[Int, String]()

  /**
   * 初始化Varys Client
   *
   * 如果是Driver端，则只生成一个Varys 客户端，用户需要在SparkConf中通过varys.master配置项
   * 配置varys master的地址
   *
   * 如果是Executor端，则从Driver的Coflow Actor中获取Coflow Id以及Varys Master的地址
   * 最后生成一个Varys Client
   */
  initialize()

  private def initialize() {
    if(isDriver) {
      varysMasterUrl = CoflowManager.getCoflowMasterUrl(conf)
    } else {
      val coflowDesc: CoflowInfo =
        askDriverWithReply[CoflowInfo](GetCoflow)
      varysMasterUrl = coflowDesc.varysMaster
      coflowId = coflowDesc.coflowId
    }
    varysClient = new VarysClient(clientName, varysMasterUrl, null)
    varysClient.start()
    logInfo(s"started varys client[name = $clientName,master url = $varysMasterUrl]")
  }
  // TODO 把注册方法放到CoflowDriverActor中统一管理
  def registerCoflow(coflowName: String,
                     maxFlows: Int,
                     coflowType: CoflowType,
                     size: Long = Int.MaxValue): String = {
    val desc = new CoflowDescription(coflowName, coflowType, maxFlows, size)
    coflowId = varysClient.registerCoflow(desc)
    logDebug(s"registered coflow[name:$coflowName,count:$maxFlows,type:$coflowType]")
    tell(RegisteredCoflow(coflowName, coflowId, varysMasterUrl))
    coflowId
  }

  def unregisterCoflow() {
    varysClient.unregisterCoflow(coflowId)
  }

  private def putFile(fileId: String, path: String, offset: Long, size: Long, numReceivers: Int) {
    if(coflowId == null) {
      throw new VarysException("try to put a file into coflow,but there is no coflow id!")
    }
//    varysClient.putFile(fileId, path, coflowId, offset, size, numReceivers)
    varysClient.putFake(fileId, coflowId, size, numReceivers)
  }

  def putBlock(blockId: String, size: Long, numReceivers: Int) {
    varysClient.putFake(blockId, coflowId, size, numReceivers)
  }

  def waitBlockReady(blockId: String) {
    varysClient.getFake(blockId, coflowId)
  }

  def putFile(fileId: String, file: File, numReceivers: Int) {
    putFile(fileId, file.getAbsolutePath, 0, file.length(), numReceivers)
  }

  def putFile(fileId: String, file: FileSegment, numReceivers: Int) {
    putFile(fileId, file.file.getAbsolutePath, file.offset, file.length, numReceivers)
  }

  def getFile(fileId: String): Array[Byte] = {
    if(coflowId == null) {
      throw new VarysException("try to get a file into coflow,but there is no coflow id!")
    }
//    varysClient.getFile(fileId, coflowId)
    varysClient.getFake(fileId, coflowId)
    Array[Byte]()
  }

  /** Send a one-way message to the master actor, to which we expect it to reply with true. */
  private def tell(message: Any) {
    if (!askDriverWithReply[Boolean](message)) {
      throw new SparkException(s"BlockManagerMasterActor returned false, expected true.[$message]")
    }
  }

  /**
   * Send a message to the driver actor and get its result within a default timeout, or
   * throw a SparkException if this fails.
   */
  private def askDriverWithReply[T](message: Any): T = {
    AkkaUtils.askWithReply(message, coflowDriverActor, AKKA_RETRY_ATTEMPTS, AKKA_RETRY_INTERVAL_MS,
      timeout)
  }
}

private[spark] object CoflowManager {
  val CoflowEnableConfig = "spark.use.coflow"
  val CoflowMasterConfig = "spark.coflow.master"

  def getCoflowMasterUrl(conf: SparkConf): String = {
    val defaultMaster: String = "varys://" + conf.get("spark.driver.host", "localhost") + ":1606"
    conf.get(CoflowMasterConfig, defaultMaster)
  }

  def useCoflow(conf: SparkConf): Boolean = {
    conf.getBoolean(CoflowEnableConfig, false)
  }
}

