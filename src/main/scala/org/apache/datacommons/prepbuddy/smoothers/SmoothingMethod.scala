package org.apache.datacommons.prepbuddy.smoothers

import org.apache.spark.rdd.RDD

abstract class SmoothingMethod extends Serializable{
    def prepare(rdd: RDD[String], windowSize: Int): RDD[Double] = {
        val duplicateRDD: RDD[(Int, String)] = rdd.mapPartitionsWithIndex((index: Int, iterator: Iterator[String]) => {
            var list: List[(Int, String)] = iterator.toList.map((index, _))
            if (index != 0) {
                val duplicates: List[(Int, String)] = list.take(windowSize).map((each) => (each._1 - 1, each._2))
                duplicates.foreach((duplicate) => list = duplicate :: list)
            }
            list.iterator
        })
        duplicateRDD.partitionBy(new KeyPartitioner(duplicateRDD.getNumPartitions)).map(_._2.toDouble)
    }
    def smooth(singleColumnDataset: RDD[String]): RDD[Double]
}
