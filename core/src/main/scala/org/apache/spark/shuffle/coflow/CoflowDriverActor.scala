package org.apache.spark.shuffle.coflow

import akka.actor.Actor
import org.apache.spark.util.ActorLogReceive
import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.storage.CoflowManagerMessages.GetCoflow
import java.util.concurrent.ConcurrentHashMap

/**
 * Created by hWX221863 on 2014/9/19.
 */
private[spark] class CoflowDriverActor(conf: SparkConf)
  extends Actor with ActorLogReceive with Logging {

  private var coflowId: String = null
  private var masterUrl: String = null

  private val coflowNameToId: ConcurrentHashMap[String, (String, String)] =
    new ConcurrentHashMap[String, (String, String)]()

  override def preStart() {

  }

  override def receiveWithLogging = {
    case GetCoflow =>
      if(coflowId == null || masterUrl == null) {
        logWarning("try to get coflow id and master,but never register coflow!")
        sender ! false
      } else {
        sender ! true
      }

    case RegisteredCoflow(coflowName_, coflowId_, masterUrl_) =>
      coflowId = coflowId_
      masterUrl = masterUrl_
      sender ! true
  }
}
