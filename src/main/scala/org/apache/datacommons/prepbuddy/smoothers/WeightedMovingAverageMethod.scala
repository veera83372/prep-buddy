package org.apache.datacommons.prepbuddy.smoothers

import org.apache.datacommons.prepbuddy.exceptions.{ErrorMessages, ApplicationException}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

class WeightedMovingAverageMethod(windowSize: Int, weights: Weights) extends SmoothingMethod {
    val slidingWindow: WeightedSlidingWindow = new WeightedSlidingWindow(windowSize, weights)

    override def smooth(singleColumnDataset: RDD[String]): RDD[Double] = {
        val duplicateRDD: RDD[Double] = prepare(singleColumnDataset, windowSize)
        duplicateRDD.mapPartitions((eachIterator) => {

            val weightedMovingAverages: ListBuffer[Double] = ListBuffer()
            eachIterator.foreach((eachValue) => {
                slidingWindow.add(eachValue)
                if (slidingWindow.isFull) weightedMovingAverages += slidingWindow.average
            })
            weightedMovingAverages.iterator
        })
    }
}
