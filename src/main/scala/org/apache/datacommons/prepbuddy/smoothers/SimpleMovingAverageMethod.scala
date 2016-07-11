package org.apache.datacommons.prepbuddy.smoothers

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

class SimpleMovingAverageMethod(windowSize: Int) extends SmoothingMethod{
    override def smooth(singleColumnDataset: RDD[String]): RDD[Double] = {
        val duplicateRDD: RDD[Double] = prepare(singleColumnDataset, windowSize)
        duplicateRDD.mapPartitions((eachPartitionsIterator) => {
            val slidingWindow: SimpleSlidingWindow = new SimpleSlidingWindow(windowSize)
            var movingAverage: ListBuffer[Double] = ListBuffer()
            eachPartitionsIterator.foreach((eachValue) => {
                slidingWindow.add(eachValue)
                if (slidingWindow.isFull()) {
                    movingAverage += slidingWindow.average
                }
            })
            movingAverage.iterator
        })
    }
}
