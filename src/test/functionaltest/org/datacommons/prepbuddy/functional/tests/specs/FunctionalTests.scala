package org.datacommons.prepbuddy.functional.tests.specs

import org.apache.spark.rdd.RDD
import org.datacommons.prepbuddy.functional.tests.framework.FunctionalTest
import org.datacommons.prepbuddy.qualityanalyzers.{DataType, MOBILE_NUMBER}
import org.datacommons.prepbuddy.rdds.TransformableRDD

object FunctionalTests extends FunctionalTest {

    test("should infer the value type to be mobile number at the specified column") {
        val testableRDD: RDD[String] = sc.textFile(datasetPath)
        val callRecords: TransformableRDD = new TransformableRDD(testableRDD)

        val actualType: DataType = callRecords.inferType(0)

        assert(MOBILE_NUMBER == actualType)
    }

    test("should deduplicate the given rdd") {
        val testableRDD: RDD[String] = sc.textFile(datasetPath)
        val callRecords: TransformableRDD = new TransformableRDD(testableRDD)

        val deduplicateRDD: TransformableRDD = callRecords.deduplicate(0 :: Nil)
        assert(deduplicateRDD.count() == 27)
    }

    printReport()
}
