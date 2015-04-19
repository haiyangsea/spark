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

import varys.framework.client.VarysClient
import java.io.File
import org.apache.spark.storage.FileSegment
import varys.framework.CoflowType._
import org.apache.spark.{SparkException, SparkConf}
import org.apache.spark.storage.ShuffleBlockId

/**
 * Created by hWX221863 on 2014/9/24.
 */
abstract class CoflowManager(executorId: String, conf: SparkConf) {
  val varysMaster: String = CoflowManager.getCoflowMasterUrl(conf)
  val clientName: String = conf.get("spark.app.name", "") + "-" + executorId
  val varysClient: VarysClient = new VarysClient(
                                      clientName,
                                      varysMaster,
                                      new CoflowClientListener)
  
  varysClient.start()

  def getCoflowId(shuffleId: Int): String

  def registerCoflow(shuffleId: Int,
                     coflowName: String,
                     maxFlows: Int,
                     coflowType: CoflowType,
                     size: Long = Int.MaxValue): String
  
  def unregisterCoflow(shuffleId: Int): Unit

  def stop(): Unit

  def putBlock(shuffleId: Int, blockId: String, size: Long, numReceivers: Int) {
    val coflowId: String = getCoflowId(shuffleId)
    varysClient.putFake(blockId, coflowId, size, numReceivers)
  }

  def waitBlockReady(shuffleId: Int, blockId: String) {
    val coflowId: String = getCoflowId(shuffleId)
    varysClient.getFake(blockId, coflowId)
  }

  def putFile(shuffleId: Int, fileId: String, file: File, numReceivers: Int) {
    val coflowId: String = getCoflowId(shuffleId)
    putFile(coflowId, fileId, file.getAbsolutePath, 0, file.length(), numReceivers)
  }

  def putFile(shuffleId: Int, fileId: String, file: FileSegment, numReceivers: Int) {
    val coflowId: String = getCoflowId(shuffleId)
    putFile(coflowId, fileId, file.file.getAbsolutePath, file.offset, file.length, numReceivers)
  }

  def getFile(shuffleId: Int, fileId: String): Array[Byte] = {
    val coflowId: String = getCoflowId(shuffleId)
    varysClient.getFile(fileId, coflowId)
  }

  def getBlock(blockId: String): Array[Byte] = {
    val shuffleBlockId = """shuffle_(\d+)_(\d+)_(\d+)""".r
    blockId match {
      case shuffleBlockId(shuffleId, mapId, reduceId) =>
        getFile(shuffleId.toInt, blockId)

      case _ =>
        throw new SparkException("The input block id[$blockId] is not shuffle block id!")
    }
  }

  private def putFile(coflowId: String,
                      fileId: String,
                      path: String,
                      offset: Long,
                      size: Long,
                      numReceivers: Int) {
    varysClient.putFile(fileId, path, coflowId, offset, size, numReceivers)
  }
}

private[spark] object CoflowManager {
  val CoflowMasterConfig = "spark.coflow.master"

  def getCoflowMasterUrl(conf: SparkConf): String = {
    val defaultMaster: String = "varys://" + conf.get("spark.driver.host", "localhost") + ":1606"
    conf.get(CoflowMasterConfig, defaultMaster)
  }

  def useCoflow(conf: SparkConf): Boolean = {
    conf.get(CoflowMasterConfig, null) != null
  }

  def makeFileId(shuffleId: Int, mapId: Int, reduceId: Int): String = {
    ShuffleBlockId(shuffleId, mapId, reduceId).name
  }

  def makeFileId(shuffleBlockId: ShuffleBlockId): String = {
    shuffleBlockId.name
  }

  def makeCoflowName(shuffleId: Int, conf: SparkConf): String = {
    conf.get("spark.app.name") + "-Shuffle[" + shuffleId + "]-Coflow"
  }
}
