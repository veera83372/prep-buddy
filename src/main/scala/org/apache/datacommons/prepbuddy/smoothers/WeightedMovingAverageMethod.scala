package org.apache.datacommons.prepbuddy.smoothers

import org.apache.datacommons.prepbuddy.exceptions.ApplicationException
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

class WeightedMovingAverageMethod(windowSize: Int, weights: Weights) extends SmoothingMethod {
    if (weights.size != windowSize){
        throw new ApplicationException("Application Exception")
    }
    override def smooth(singleColumnDataset: RDD[String]): RDD[Double] = {
        val duplicateRDD: RDD[Double] = prepare(singleColumnDataset, windowSize)
        duplicateRDD.mapPartitions((eachIterator) => {
            val slidingWindow: WeightedSlidingWindow = new WeightedSlidingWindow(windowSize, weights)
            val weightedMovingAverages: ListBuffer[Double] = ListBuffer()
            eachIterator.foreach((eachValue) => {
                slidingWindow.add(eachValue)
                if (slidingWindow.isFull) weightedMovingAverages += slidingWindow.average
            })
            weightedMovingAverages.iterator
        })
    }
}
