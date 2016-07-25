package org.apache.datacommons.prepbuddy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class DFTestCase extends FunSuite with BeforeAndAfterEach {
    protected var sparkContext: SparkContext = null
    protected var sqlContext: SQLContext = null

    override def beforeEach() {
        val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local")
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        sparkContext = new SparkContext(sparkConf)
        sqlContext = new SQLContext(sparkContext)
    }

    override def afterEach() {
        sparkContext.stop()
    }
}
