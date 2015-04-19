/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.coflow

import akka.actor.ActorRef
import org.apache.spark.util.AkkaUtils
import org.apache.spark.SparkConf
import varys.framework.CoflowType._
import org.apache.spark.shuffle.coflow.CoflowManagerMessages.CoflowInfo
import org.apache.spark.shuffle.coflow.CoflowManagerMessages.GetCoflowInfo
import scala.collection.mutable

/**
 * Created by hWX221863 on 2014/9/24.
 */
class CoflowManagerSlave(driverActor: ActorRef, executorId: String, conf: SparkConf)
  extends CoflowManager("Executor-" +executorId, conf) {
  private val AKKA_RETRY_ATTEMPTS: Int = AkkaUtils.numRetries(conf)
  private val AKKA_RETRY_INTERVAL_MS: Int = AkkaUtils.retryWaitMs(conf)
  val timeout = AkkaUtils.askTimeout(conf)

  private val shuffleToCoflow: mutable.HashMap[Int, String] =
    new mutable.HashMap[Int, String]

  override def getCoflowId(shuffleId: Int): String = {
    shuffleToCoflow.getOrElse(shuffleId, {
      val coflowId: String = fetchCoflowId(shuffleId)
      shuffleToCoflow += shuffleId -> coflowId
      coflowId
    })
  }

  private def fetchCoflowId(shuffleId: Int): String = {
    val coflowInfo: CoflowInfo = AkkaUtils.askWithReply[CoflowInfo](
      GetCoflowInfo(shuffleId),
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
