package org.apache.spark.shuffle.coflow

import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.scalatest.FunSuite

/**
 * Created by Allen on 2015/4/19.
 */
class CoflowManagerTest extends FunSuite {
  test("test CoflowManager object") {
    assert(CoflowManager.getBlockId("shuffle_11_12_3") == ShuffleBlockId(11, 12, 3))
    assert(CoflowManager.getBlockId("shuffle_9_1_13") == ShuffleBlockId(9, 1, 13))
  }

  test("local cluster") {
    System.setProperty("varys.client.data.locality", "false")
    val coflowMaster = s"varys://${Utils.localIpAddress()}:1606"
    val conf = new SparkConf
    conf.setMaster("local[2]")
      .setAppName("coflow")
      .set("spark.coflow.master", coflowMaster)
      .set("spark.shuffle.manager", "hash")

    val sc = new SparkContext(conf)

    val f = sc.parallelize(Array(1, 2, 2))
    val result = f.map((_, 1)).groupByKey(2).map {
      case (key, counts) => (key, counts.reduce(_ + _))
    }
    result.foreach(println)
  }
}
