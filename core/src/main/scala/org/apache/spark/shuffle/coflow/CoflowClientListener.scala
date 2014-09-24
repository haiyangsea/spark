package org.apache.spark.shuffle.coflow

import varys.framework.client.ClientListener
import org.apache.spark.{SparkException, Logging}

/**
 * Created by hWX221863 on 2014/9/24.
 */
class CoflowClientListener extends ClientListener with Logging {
  // NOT SAFE to use the Client UNTIL this method is called
  override def connected(clientId: String): Unit = {
      logInfo("varys client has connected to master,got client id " + clientId)
  }

  override def disconnected(): Unit = {
    logInfo("varys client disconnected from master")
  }

  // Only called for deadline-sensitive coflows
  override def coflowRejected(coflowId: String, rejectMessage: String): Unit = {
    throw new SparkException(s"coflow[id = $coflowId] was rejected by master,message: $rejectMessage")
  }
}
