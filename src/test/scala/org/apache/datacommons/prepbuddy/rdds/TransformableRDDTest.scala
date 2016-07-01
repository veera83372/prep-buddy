package org.apache.datacommons.prepbuddy.rdds

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.datacommons.prepbuddy.cluster.TextFacets
import org.apache.datacommons.prepbuddy.types.CSV
import org.apache.spark.rdd.RDD
import org.junit.Assert._


class TransformableRDDTest extends SparkTestCase {

    test("should be able to count on transformableRdd") {
        val data = Array("1,23", "2,45", "3,65", "4,67", "5,23")
        val dataSet: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataSet, CSV)
        assert(5 == transformableRDD.count())
    }

    test("should deduplicate a dataset by considering all the columns") {
        val records: Array[String] = Array(
            "Smith,Male,USA,12345",
            "John,Male,USA,12343",
            "John,Male,USA,12343",
            "Smith,Male,USA,12342",
            "John,Male,India,12343",
            "Smith,Male,USA,12342"
        )
        val initialDataset: RDD[String] = sparkContext.parallelize(records)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val deduplicatedRDD: TransformableRDD = initialRDD.deduplicate()
        assert(4 == deduplicatedRDD.count)
    }

    test("should deduplicate a dataset by considering the given columns as primary key") {
        val records: Array[String] = Array(
            "Smith,Male,USA,12345",
            "John,Male,USA,12343",
            "John,Male,USA,12343",
            "Smith,Male,USA,12342",
            "John,Male,India,12343",
            "Smith,Male,USA,12342"
        )
        val initialDataset: RDD[String] = sparkContext.parallelize(records)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val deduplicatedRDD: TransformableRDD = initialRDD.deduplicate(List(0, 1))

        assertEquals(2, deduplicatedRDD.count)
    }

    test("should drop the specified column from the given rdd") {
        val data = Array(
            "John,Male,21,Canada",
            "Smith, Male, 30, UK",
            "Larry, Male, 23, USA",
            "Fiona, Female,18,USA"
        )
        val dataset: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataset, CSV)
        val transformedRows: Array[String] = transformableRDD.dropColumn(2).collect()

        assert(transformedRows.contains("John,Male,Canada"))
        assert(transformedRows.contains("Smith, Male, UK"))
        assert(transformedRows.contains("Larry, Male, USA"))
        assert(transformedRows.contains("Fiona, Female,USA"))
    }

    test("toDoubleRdd should give double RDD of given column index") {
        val data = Array("1,23", "2,45", "3,65", "4,67", "5,23")
        val dataSet: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataSet, CSV)
        val doubleRdd: RDD[Double] = transformableRDD.toDoubleRDD(0)
        val collected: Array[Double] = doubleRdd.collect()
        val expected: Double = 3

        assert(collected.contains(expected))
        assert(collected.contains(1))
        assert(collected.contains(2))
        assert(collected.contains(4))
        assert(collected.contains(5))
    }

    test("text facet should give count of Pair") {
        val initialDataset: RDD[String] = sparkContext.parallelize(Array("X,Y", "A,B", "X,Z", "A,Q", "A,E"))
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val textFacets: TextFacets = initialRDD.listFacets(0)
        assertEquals(2, textFacets.count)
    }
}
