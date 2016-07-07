package org.apache.datacommons.prepbuddy.smoothers

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.spark.rdd.RDD
import org.junit.Assert

class SmoothingMethodTest extends SparkTestCase{
    test("should be able to duplicate data of given window size to previous partition") {
        val data = Array("1", "2", "3")
        val dataSet: RDD[String] = sparkContext.parallelize(data, 3)
        val method: SmoothingMethod = new SmoothingMethod{
            override def smooth(singleColumnDataset: RDD[String]): RDD[Double] = ???
        }
        val doubleRdd: RDD[Double] = method.prepare(dataSet, 3)
        val collected: Array[Double] = doubleRdd.collect()
        assert(5 == collected.length)
    }

    test("should be able to smooth by simple moving average method") {
        val initialDataset: RDD[String] = sparkContext.parallelize(Array("3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14"), 3)
        val movingAverage: SimpleMovingAverageMethod = new SimpleMovingAverageMethod(3)
        val rdd: RDD[Double] = movingAverage.smooth(initialDataset)

        val averages: Array[Double] = rdd.collect()
        val expected: Double = 4.0

        assert(averages.contains(expected))
        assert(averages.contains(5.0))
        assert(averages.contains(6.0))
        assert(averages.contains(7.0))
        assert(averages.contains(8.0))
        assert(averages.contains(9.0))
        assert(averages.contains(10.0))
        assert(averages.contains(11.0))
        assert(averages.contains(12.0))
        assert(averages.contains(13.0))

        assert(!averages.contains(14.0))
    }

    test("should smooth the values by weighted moving average method") {
        val initialDataset: RDD[String] = sparkContext.parallelize(Array("10", "12", "16", "13", "17", "19", "15", "20", "22", "19", "21", "19"), 3)

        val weights: Weights = new Weights(3)
        weights.add(0.166)
        weights.add(0.333)
        weights.add(0.5)

        val movingAverage: WeightedMovingAverageMethod = new WeightedMovingAverageMethod(3, weights)
        val rdd: RDD[Double] = movingAverage.smooth(initialDataset)

        val expected: Double = 13.66
        val actual: Double = "%1.2f".format(rdd.first).toDouble
        assert(expected == actual)
    }

}
