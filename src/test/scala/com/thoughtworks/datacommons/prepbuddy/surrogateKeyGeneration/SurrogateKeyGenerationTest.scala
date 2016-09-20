package com.thoughtworks.datacommons.prepbuddy.surrogateKeyGeneration

import com.thoughtworks.datacommons.prepbuddy.SparkTestCase
import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.rdd.RDD

class SurrogateKeyGenerationTest extends SparkTestCase {
    test("should add surrogate key at the beginning of the row with incremental value greater than the offset") {
        val dataset: RDD[String] = sparkContext.parallelize(Array(
            "One,Two,Three",
            "Four,Five,Six",
            "Seven,Eight,Nine",
            "Ten,Eleven,Twelve"
        ), 3)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataset)
        val surrogateKeys: Set[String] = transformableRDD.addSurrogateKey(100).select(0).collect().toSet

        val expected: Set[String] = (101 to 104).map(_.toString).toSet

        assertResult(4)(surrogateKeys.size)
        assertResult(expected)(surrogateKeys)
    }

    test("should add UUID as surrogate key at the beginning of the row") {
        val dataset: RDD[String] = sparkContext.parallelize(Array(
            "One,Two,Three",
            "Four,Five,Six",
            "Seven,Eight,Nine",
            "Ten,Eleven,Twelve"
        ), 3)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataset)
        val surrogateKeys: Array[String] = transformableRDD.addSurrogateKey().select(0).collect()

        assertResult(4)(surrogateKeys.distinct.length)
    }
}
