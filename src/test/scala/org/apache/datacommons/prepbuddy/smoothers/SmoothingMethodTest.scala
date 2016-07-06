package org.apache.datacommons.prepbuddy.smoothers

import java.util.Arrays

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

//        val expected: Double = 4.0
//        assert(expected == rdd.first)
    }

}
