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
