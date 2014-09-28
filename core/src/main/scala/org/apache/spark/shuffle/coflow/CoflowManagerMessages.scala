package org.apache.spark.shuffle.coflow

/**
 * Created by hWX221863 on 2014/9/19.
 */
private[spark] object CoflowManagerMessages {

  // ===================================================================================
  // Executor To Driver Messages
  // ===================================================================================
  sealed trait ToCoflowMaster

  case class GetCoflowInfo(shuffleId: Int) extends ToCoflowMaster

  case object StopCoflowMaster extends ToCoflowMaster

  // ===================================================================================
  // Driver To Executor Messages
  // ===================================================================================
  sealed trait ToCoflowSlave
  
  case class CoflowInfo(coflowId: String) extends ToCoflowSlave
}
