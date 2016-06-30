package org.apache.datacommons.prepbuddy.cleansers.imputation

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.types.CSV
import org.apache.datacommons.prepbuddy.utils.RowRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class ImputationTest extends FunSuite with BeforeAndAfterEach {
    var sparkContext: SparkContext = null

    override def beforeEach() {
        val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local")
        sparkContext = new SparkContext(sparkConf)
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
    }

    override def afterEach() {
        sparkContext.stop()
    }

    test("should impute value with returned value of strategy") {
        val data = Array("1,", "2,45", "3,65", "4,67", "5,23")
        val dataSet: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataSet, CSV)
        val imputed: TransformableRDD = transformableRDD.impute(1, new ImputationStrategy {
            override def handleMissingData(record: RowRecord): String = "hello"

            override def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int): Unit = {}
        })
        val collected: Array[String] = imputed.collect()
        assert(collected.contains("1,hello"))
        assert(collected.contains("2,45"))
    }
}
