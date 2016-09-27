package com.thoughtworks.datacommons.prepbuddy.imputations

import com.thoughtworks.datacommons.prepbuddy.SparkTestCase
import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import com.thoughtworks.datacommons.prepbuddy.types.CSV
import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class ImputationTest extends SparkTestCase {

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

    test("should impute value with returned value of strategy by referring to the column by name") {
        val data = Array("1,", "2,45", "3,65", "4,67", "5,23")
        val dataSet: RDD[String] = sparkContext.parallelize(data)
        val schema: Map[String, Int] = Map("First" -> 0, "Second" -> 1)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataSet, CSV).useSchema(schema)
        val imputed: TransformableRDD = transformableRDD.impute("Second", new ImputationStrategy {
            override def handleMissingData(record: RowRecord): String = "hello"

            override def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int): Unit = {}
        })
        val collected: Array[String] = imputed.collect()
        assert(collected.contains("1,hello"))
        assert(collected.contains("2,45"))
    }

    test("should impute the missing values by considering missing hints") {
        val initialDataSet: RDD[String] = sparkContext.parallelize(Array(
            "1,NULL,2,3,4", "2,N/A,23,21,23",
            "3,N/A,21,32,32", "4,-,2,3,4",
            "5,,54,32,54", "6,32,22,33,23"))
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataSet)

        val imputed: Array[String] = initialRDD.impute(1, new ImputationStrategy() {
            def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int) {
            }

            def handleMissingData(record: RowRecord): String = {
                "X"
            }
        }, List("N/A", "-", "NA", "NULL")).collect

        assert(imputed.contains("1,X,2,3,4"))
        assert(imputed.contains("2,X,23,21,23"))
        assert(imputed.contains("3,X,21,32,32"))
        assert(imputed.contains("4,X,2,3,4"))
        assert(imputed.contains("5,X,54,32,54"))
        assert(imputed.contains("6,32,22,33,23"))
    }

    test("should impute the missing values by considering missing hints when column is specified by name") {
        val initialDataSet: RDD[String] = sparkContext.parallelize(Array(
            "1,NULL,2,3,4", "2,N/A,23,21,23",
            "3,N/A,21,32,32", "4,-,2,3,4",
            "5,,54,32,54", "6,32,22,33,23"))
        val schema: Map[String, Int] = Map("First" -> 0, "Second" -> 1, "Third" -> 2, "Fourth" -> 3, "Fifth" -> 4)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataSet).useSchema(schema)

        val imputed: Array[String] = initialRDD.impute("Second", new ImputationStrategy() {
            def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int) {
            }

            def handleMissingData(record: RowRecord): String = {
                "X"
            }
        }, List("N/A", "-", "NA", "NULL")).collect

        assert(imputed.contains("1,X,2,3,4"))
        assert(imputed.contains("2,X,23,21,23"))
        assert(imputed.contains("3,X,21,32,32"))
        assert(imputed.contains("4,X,2,3,4"))
        assert(imputed.contains("5,X,54,32,54"))
        assert(imputed.contains("6,32,22,33,23"))
    }

    test("should impute missing values by mean of the given column index ") {
        val data = Array("1,", "2,45", "3,65", "4,67", "5,23")
        val dataSet: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataSet, CSV)
        val imputedByMean: TransformableRDD = transformableRDD.impute(1, new MeanSubstitution())
        val collected: Array[String] = imputedByMean.collect()

        assert(collected.contains("1,50.0"))
        assert(collected.contains("2,45"))
    }

    test("should impute the missing values by approx mean") {
        val data = Array("1,", "2,45", "3,65", "4,67", "5,23")
        val dataSet: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataSet, CSV)
        val imputedByMean: TransformableRDD = transformableRDD.impute(1, new ApproxMeanSubstitution)
        val collected: Array[String] = imputedByMean.collect()

        assert(collected.contains("1,50.0"))
        assert(collected.contains("5,23"))
    }

    test("should impute the missing values by mode") {
        val data = Array("1,", "2,45", "3,45", "4,", "5,23")
        val dataSet: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataSet, CSV)
        val imputedByMean: TransformableRDD = transformableRDD.impute(1, new ModeSubstitution())
        val collected: Array[String] = imputedByMean.collect()

        assert(collected.contains("1,45"))
        assert(collected.contains("4,45"))
    }

    test("should impute by naive bayes substitution") {
        val dataset: mutable.WrappedArray[String] = {
            Array("sunny,hot,high,false,N",
                "sunny,hot,high,true,N",
                "overcast,hot,high,false,P",
                "rain,mild,high,false,P",
                "rain,cool,normal,false,P",
                "rain,cool,normal,true,N",
                "overcast,cool,normal,true,P",
                "sunny,mild,high,false,N",
                "sunny,cool,normal,false,P",
                "rain,mild,normal,false,P",
                "sunny,mild,normal,true,P",
                "overcast,mild,high,true,P",
                "overcast,hot,normal,false,P",
                "rain,mild,high,true,N")
        }
        val initialDataSet: RDD[String] = sparkContext.parallelize(dataset)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataSet)

        val naiveBayesSubstitution: NaiveBayesSubstitution = new NaiveBayesSubstitution(Array(0, 1, 2, 3))
        naiveBayesSubstitution.prepareSubstitute(initialRDD, 4)

        var rowRecord: Array[String] = "sunny,cool,high,false".split(",")
        val mostProbable: String = {
            naiveBayesSubstitution.handleMissingData(new RowRecord(rowRecord))
        }

        assert("N" == mostProbable)
        rowRecord = "rain,hot,high,false".split(",")
        assert("N" == naiveBayesSubstitution.handleMissingData(new RowRecord(rowRecord)))

        val record: Array[String] = "overcast, hot, high, true".split(",")
        assert("P" == naiveBayesSubstitution.handleMissingData(new RowRecord(record)))
    }

    test("should impute by linear regression") {
        val initialDataSet: RDD[String] = {
            sparkContext.parallelize(Array("60,3.1", "61,3.6", "62,3.8", "63,4", "65,4.1"))
        }
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataSet)
        val strategy: UnivariateLinearRegressionSubstitution = new UnivariateLinearRegressionSubstitution(0)
        strategy.prepareSubstitute(initialRDD, 1)

        val record: Array[String] = Array[String]("64")
        val expected: String = "4.06"

        assert(expected == strategy.handleMissingData(new RowRecord(record)))

        val emptyValue: Array[String] = Array[String]("")
        val expected1: String = ""

        assert(expected1 == strategy.handleMissingData(new RowRecord(emptyValue)))

    }

}
