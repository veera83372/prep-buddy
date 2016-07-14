package org.apache.datacommons.prepbuddy.examples

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.smoothers.SimpleMovingAverageMethod
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SmootherMain {
    def main(args: Array[String]) {
        if (args.length == 0) {
            System.out.println("--> File Path Need To Be Specified")
            System.exit(0)
        }
        val conf: SparkConf = new SparkConf().setAppName("Smmother Main")
        val sc: SparkContext = new SparkContext(conf)
        val filePath: String = args(0)
        val csvInput: RDD[String] = sc.textFile(filePath, args(1).toInt)
        val movingAverage: SimpleMovingAverageMethod = new SimpleMovingAverageMethod(3)
        val d: TransformableRDD = new TransformableRDD(csvInput)
        val smooth: RDD[Double] = d.smooth(3, movingAverage)
        println("Smoother Count" + smooth.count)
        sc.stop()
    }
}
