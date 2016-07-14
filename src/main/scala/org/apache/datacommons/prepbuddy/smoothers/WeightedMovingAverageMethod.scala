package org.apache.datacommons.prepbuddy.smoothers

import org.apache.datacommons.prepbuddy.exceptions.{ApplicationException, ErrorMessages}
import org.apache.spark.rdd.RDD

class WeightedMovingAverageMethod(windowSize: Int, weights: Weights) extends SmoothingMethod {
    if (windowSize != weights.size) {
        throw new ApplicationException(ErrorMessages.WINDOW_SIZE_AND_WEIGHTS_SIZE_NOT_MATCHING)
    }

    override def smooth(singleColumnDataset: RDD[String]): RDD[Double] = {
        val duplicateRDD: RDD[Double] = prepare(singleColumnDataset, windowSize)
        duplicateRDD.mapPartitions(_.sliding(windowSize).map(average))
    }

    private def average(windowValues: Seq[Double]) = weights.multiplyWith(windowValues.toList).sum
}
