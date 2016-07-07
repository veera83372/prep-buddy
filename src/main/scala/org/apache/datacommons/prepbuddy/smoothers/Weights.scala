package org.apache.datacommons.prepbuddy.smoothers

import scala.collection.mutable

class Weights(limit: Int) extends Serializable {
    private var weights: List[Double] = List()

    def multiplyWith(queue: mutable.Queue[Double]): List[Double] = {
        weights.zip(queue).map((tuple) => tuple._1 * tuple._2)
    }

    def get(index: Int): Double = weights(index)

    def add(value: Double): Unit = weights = weights :+ value

}
