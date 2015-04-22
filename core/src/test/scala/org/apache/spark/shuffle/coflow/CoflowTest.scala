package org.apache.spark.shuffle.coflow

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.util.Utils
import org.scalatest.FunSuite

/**
 * Created by hWX221863 on 2015/4/21.
 */
class CoflowTest extends FunSuite {
  test("local test") {
    val coflowMaster = "varys://HGHY1hwx2218631:1606" //s"varys://${Utils.localHostName()}:1606"
    val conf = new SparkConf
    conf.setMaster("local[2]")
      .setAppName("coflow")
      .set("spark.coflow.master", coflowMaster)

    val sc = new SparkContext(conf)
    val count = sc.parallelize(Array(1,2,3,1,2)).map((_, 1)).groupByKey(2).count()
    println(count)
  }
}
