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

import akka.actor.{Props, ActorSystem, Actor, ActorRef}
import org.apache.spark.{SparkException, SparkConf, Logging}
import org.apache.spark.util.{ActorLogReceive, AkkaUtils}
import varys.framework.CoflowType.CoflowType
import varys.framework.CoflowDescription
import scala.collection.mutable
import org.apache.spark.shuffle.coflow.CoflowManagerMessages._
import java.util.concurrent.TimeoutException
import scala.concurrent.Await

/**
 * Created by hWX221863 on 2014/9/19.
 */
private[spark]
class CoflowManagerMaster(
    actorSystem: ActorSystem,
    conf: SparkConf)
  extends CoflowManager("Driver", conf) with Logging {

  private val shuffleCoflowPair: mutable.HashMap[Int, String] =
    new mutable.HashMap[Int, String]()

  private var actor: ActorRef = null

  initialize()

  private def initialize() {
    actor = actorSystem.actorOf(Props(new CoflowDriverActor), CoflowManagerMaster.DriverActorName)
  }

  override def stop() {
    varysClient.stop()
    if (actor != null) {
      import akka.pattern.ask

      try {
        val timeout = AkkaUtils.askTimeout(conf)
        val future = actor.ask(StopCoflowMaster)(timeout)
        Await.result(future, timeout)
      } catch {
        case e: TimeoutException =>
          logInfo("Stop request to Master timed out; it may already be shut down.")
      }
      actor = null
    }
  }

  class CoflowDriverActor extends Actor with ActorLogReceive with Logging {
    override def receiveWithLogging = {
      case GetCoflowInfo(shuffleId) =>
          val coflowId: String = shuffleCoflowPair.getOrElse(shuffleId, null)
          sender ! CoflowInfo(coflowId)

      case StopCoflowMaster =>
        sender ! true
        context.stop(self)
    }
  }

  def registerCoflow(shuffleId: Int,
                     coflowName: String,
                     maxFlows: Int,
                     coflowType: CoflowType,
                     size: Long = Int.MaxValue): String = {
    val desc = new CoflowDescription(coflowName, coflowType, maxFlows, size)
    val coflowId = varysClient.registerCoflow(desc)
    shuffleCoflowPair += shuffleId -> coflowId
    coflowId
  }

  def unregisterCoflow(shuffleId: Int) {
    val coflowId: Option[String] = shuffleCoflowPair.get(shuffleId)
    coflowId.foreach(id => {
      varysClient.unregisterCoflow(id)
      logInfo(s"coflow[id = $id] has been unregistered!")
    })

  }

  override def getCoflowId(shuffleId: Int): String = {
    val coflowId: Option[String] = shuffleCoflowPair.get(shuffleId)
    if(!coflowId.isDefined) {
      throw new SparkException("try to get a never register coflow id[shuffle id = " + shuffleId)
    }
    coflowId.get
  }
}

object CoflowManagerMaster {
  val DriverActorName: String = "CoflowDriverActor"
}