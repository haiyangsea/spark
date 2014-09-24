package org.apache.spark.shuffle.coflow

import akka.actor.ActorRef
import varys.framework.client.VarysClient
import org.apache.spark.util.AkkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.shuffle.coflow.CoflowManagerMessages.{GetCoflowInfo, CoflowInfo}
import varys.framework.CoflowType._
import org.apache.spark.shuffle.coflow.CoflowManagerMessages.CoflowInfo
import org.apache.spark.shuffle.coflow.CoflowManagerMessages.GetCoflowInfo

/**
 * Created by hWX221863 on 2014/9/24.
 */
class CoflowManagerSlave(driverActor: ActorRef, varysClient: VarysClient, conf: SparkConf)
  extends CoflowManager(varysClient) {
  private val AKKA_RETRY_ATTEMPTS: Int = AkkaUtils.numRetries(conf)
  private val AKKA_RETRY_INTERVAL_MS: Int = AkkaUtils.retryWaitMs(conf)
  val timeout = AkkaUtils.askTimeout(conf)

  override def getCoflowId(shuffleId: Int): String = {
    val coflowInfo: CoflowInfo = AkkaUtils.askWithReply[CoflowInfo](GetCoflowInfo,
      driverActor,
      AKKA_RETRY_ATTEMPTS,
      AKKA_RETRY_INTERVAL_MS,
      timeout)
    coflowInfo.coflowId
  }

  override def unregisterCoflow(shuffleId: Int) {
    // do nothing
  }

  override def registerCoflow(shuffleId: Int,
                              coflowName: String,
                              maxFlows: Int,
                              coflowType: CoflowType,
                              size: Long = Int.MaxValue): String = {
    // do nothing here
    throw new NotImplementedError("slave shouldn't call this method")
  }

  override def stop() {
    varysClient.stop()
  }
}
