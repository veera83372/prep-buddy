package com.thoughtworks.datacommons.prepbuddy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class SparkTestCase extends FunSuite with BeforeAndAfterEach {
    var sparkContext: SparkContext = _
    
    override def beforeEach() {
        val spark: SparkSession = SparkSession
            .builder()
            .appName(getClass.getCanonicalName)
            .master("local[2]")
            .getOrCreate()
        sparkContext = spark.sparkContext
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
    }
}
