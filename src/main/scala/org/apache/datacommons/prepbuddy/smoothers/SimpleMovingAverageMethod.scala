package org.apache.datacommons.prepbuddy.smoothers

import org.apache.spark.rdd.RDD

class SimpleMovingAverageMethod(windowSize: Int) extends SmoothingMethod{
    override def smooth(singleColumnDataset: RDD[String]): RDD[Double] = {
        val duplicateRDD: RDD[Double] = prepare(singleColumnDataset, windowSize)
//        duplicateRDD.mapPartitions((eachItretor) => {
//            val slidingWindow: SimpleSlidingWindow = new SimpleSlidingWindow(windowSize)
//            val movingAverage: List[Double] = List.empty()
//            eachItretor.foreach((eachValue) => {
//                slidingWindow.add(eachValue)
//                if (slidingWindow.isFull)
//                    movingAverage += slidingWindow.average
//            })
//            movingAverage
//        })
        singleColumnDataset.map(_.toDouble)
    }
}
