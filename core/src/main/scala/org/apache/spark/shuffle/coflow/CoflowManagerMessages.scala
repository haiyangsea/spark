package org.apache.spark.shuffle.coflow

/**
 * Created by hWX221863 on 2014/9/19.
 */
private[spark] object CoflowManagerMessages {

  sealed trait ToCoflowDriver
  case object GetCoflow extends ToCoflowDriver
  case object GetCoflowMaster extends ToCoflowDriver
  case class RegisteredCoflow(name: String, id: String, masterUrl: String) extends ToCoflowDriver


  sealed trait ToCoflowExecutor
  case class CoflowInfo(varysMaster: String, coflowId: String) extends ToCoflowExecutor
}
