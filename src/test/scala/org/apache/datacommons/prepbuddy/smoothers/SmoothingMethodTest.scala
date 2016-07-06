package org.apache.datacommons.prepbuddy.smoothers

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.spark.rdd.RDD

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


}
