package com.thoughtworks.datacommons.prepbuddy.exceptions

import com.thoughtworks.datacommons.prepbuddy.SparkTestCase
import com.thoughtworks.datacommons.prepbuddy.clusterers.SimpleFingerprintAlgorithm
import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import com.thoughtworks.datacommons.prepbuddy.smoothers.{SimpleMovingAverageMethod, WeightedMovingAverageMethod, Weights}
import com.thoughtworks.datacommons.prepbuddy.utils.Probability
import org.apache.spark.rdd.RDD

class ExceptionTest extends SparkTestCase {
    test("Weighted moving average should not be creatable when weights and window size is not equal") {
        val weights: Weights = new Weights(3)
        val thrown = intercept[ApplicationException] {
            new WeightedMovingAverageMethod(1, weights)
        }
        assert(thrown.getMessage == "Window size and weighs size should be same.")
    }

    test("weights add should throw exception when sum of weights is not equal to one") {
        val weights: Weights = new Weights(3)
        weights.add(0.166)
        weights.add(0.333)
        weights.add(0.5)

        val otherWeights: Weights = new Weights(2)
        otherWeights.add(0.777)
        val thrown = intercept[ApplicationException] {
            otherWeights.add(0.333)
        }
        assert(thrown.getMessage == "To calculate weighted moving average weights sum should be up to one.")
    }

    test("weights should throw exception if size is exceeded") {
        val weights: Weights = new Weights(2)
        weights.add(0.5)
        weights.add(0.5)
        val thrown = intercept[ApplicationException] {
            weights.add(0.3)
        }
        assert(thrown.getMessage == "Can not add value more than size limit.")
    }

    test("Probability should not create new object if probability value is not valid") {
        val thrown = intercept[ApplicationException] {
            new Probability(1.1)
        }
        assert(thrown.getMessage == "Probability can not be less than zero or greater than 1")

        val otherThrown = intercept[ApplicationException] {
            new Probability(-1)
        }
        assert(otherThrown.getMessage == "Probability can not be less than zero or greater than 1")
    }

    test("smooth should throw exception if given column is not numeric") {
        val data = Array(
            "one two, three",
            "two one, four"
        )
        val initialDataset: RDD[String] = sparkContext.parallelize(data)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val thrown = intercept[ApplicationException] {
            initialRDD.smooth(0, new SimpleMovingAverageMethod(2))
        }
        assert(thrown.getMessage == "Values of column are not numeric")
    }

    test("toDoubleRDD should throw exception if given column is not numeric") {
        val data = Array(
            "one two, three",
            "two one, four"
        )
        val initialDataset: RDD[String] = sparkContext.parallelize(data)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val thrown = intercept[ApplicationException] {
            initialRDD.toDoubleRDD(0)
        }
        assert(thrown.getMessage == "Values of column are not numeric")
    }

    test("should throw an exception while selecting column by column name when schema is not set") {
        val dataSet: RDD[String] = sparkContext.parallelize(Array(
            "2, 1, 4, 4",
            "4, 5, 5, 6",
            "7, 7, 7, 9",
            "1, 9, 4, 9"
        ))

        val transformableRDD: TransformableRDD = new TransformableRDD(dataSet)

        val thrown = intercept[ApplicationException] {
            val secondColumnValues: RDD[String] = transformableRDD.select("Second")
        }
        assert(thrown.getMessage == "Schema is not set")
    }

    test("should throw COLUMN_NOT_FOUND exception when column name does not exists in the schema") {
        val dataSet: RDD[String] = sparkContext.parallelize(Array(
            "2, 1, 4, 4",
            "4, 5, 5, 6",
            "7, 7, 7, 9",
            "1, 9, 4, 9"
        ))
        val schema: Map[String, Int] = Map("First" -> 0, "Second" -> 1, "Third" -> 2, "Fourth" -> 3)

        val transformableRDD: TransformableRDD = new TransformableRDD(dataSet).useSchema(schema)

        val thrown = intercept[ApplicationException] {
            val secondColumnValues: RDD[String] = transformableRDD.select("Tenth")
        }
        assert(thrown.getMessage == "No such column found for the current data set")
    }

    test("should throw COLUMN_NOT_FOUND exception while selecting column by name with invalid index reference") {
        val dataSet: RDD[String] = sparkContext.parallelize(Array(
            "2, 1, 4, 4",
            "4, 5, 5, 6",
            "7, 7, 7, 9",
            "1, 9, 4, 9"
        ))
        val schema: Map[String, Int] = Map("First" -> 0, "Second" -> 1, "Third" -> 2, "Fourth" -> 10)

        val transformableRDD: TransformableRDD = new TransformableRDD(dataSet).useSchema(schema)

        val thrown = intercept[ApplicationException] {
            val secondColumnValues: RDD[String] = transformableRDD.select("Fourth")
        }
        assert(thrown.getMessage == "No such column found for the current data set")
    }

    test("should throw COLUMN_NOT_FOUND exception when the given index is out of the column length in the data set") {
        val initialDataSet: RDD[String] = sparkContext.parallelize(Array(
            "1,NULL,2,3,4", "2,N/A,23,21,23",
            "3,N/A,21,32,32", "4,-,2,3,4",
            "5,,54,32,54", "6,32,22,33,23"))
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataSet)

        val thrown: ApplicationException = intercept[ApplicationException] {
            initialRDD.clusters(5, new SimpleFingerprintAlgorithm())
        }

        assert(thrown.getMessage == "No such column found for the current data set")
    }

    test("should throw COLUMN_NOT_FOUND exception when negative column index is passed to cluster") {
        val initialDataSet: RDD[String] = sparkContext.parallelize(Array(
            "1,NULL,2,3,4", "2,N/A,23,21,23",
            "3,N/A,21,32,32", "4,-,2,3,4",
            "5,,54,32,54", "6,32,22,33,23"))
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataSet)

        val thrown: ApplicationException = intercept[ApplicationException] {
            initialRDD.clusters(-10, new SimpleFingerprintAlgorithm())
        }

        assert(thrown.getMessage == "No such column found for the current data set")
    }
}
