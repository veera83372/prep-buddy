package org.apache.datacommons.prepbuddy.smoothers

import org.apache.spark.rdd.RDD

class SimpleMovingAverageMethod(windowSize: Int) extends SmoothingMethod {
    override def smooth(singleColumnDataset: RDD[String]): RDD[Double] = {
        val duplicateRDD: RDD[Double] = prepare(singleColumnDataset, windowSize)
        duplicateRDD.mapPartitions(_.sliding(windowSize).map(average))
    }

    private def average(windowValues: Seq[Double]) = windowValues.sum / windowSize

}
