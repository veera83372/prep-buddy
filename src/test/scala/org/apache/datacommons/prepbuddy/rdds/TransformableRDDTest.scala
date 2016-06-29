package org.apache.datacommons.prepbuddy.rdds

import org.apache.datacommons.prepbuddy.types.CSV
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}


class TransformableRDDTest extends FunSuite with BeforeAndAfterEach{

  var sc :SparkContext = null
  override def beforeEach() {
    val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  override def afterEach() {
    sc.stop()
  }

  test ("should be able to count on encryptedRdd"){
    val data = Array("1,23", "2,45", "3,65", "4,67", "5,23")
    val dataSet: RDD[String] = sc.parallelize(data)
    val transformableRDD: TransformableRDD = new TransformableRDD(dataSet,CSV)
    assert(5==transformableRDD.count())
  }
}
