package org.apache.spark.shuffle.coflow

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.scalatest.FunSuite

/**
 * Created by Allen on 2015/4/19.
 */
class CoflowManagerTest extends FunSuite {
  test("test CoflowManager object") {
    val flowId = "shuffle_0_1_2"
    val blockId = CoflowManager.getBlockId(flowId)
    assert(blockId.shuffleId == 0)
    assert(blockId.mapId == 1)
    assert(blockId.reduceId == 2)
  }

  test("local cluster") {
//    System.setProperty("varys.client.data.locality", "false")
    val coflowMaster = "varys://Allen:1606"
    val conf = new SparkConf
    conf.setMaster("local[2]").setAppName("coflow").set("spark.coflow.master", coflowMaster)

    val sc = new SparkContext(conf)

    val f = sc.parallelize(Array("12.3", "12.4", "13.4"))
    val result = f.flatMap(line => line.split(".")).map((_, 1)).groupByKey(1).take(1)
    println(result)
  }
}
