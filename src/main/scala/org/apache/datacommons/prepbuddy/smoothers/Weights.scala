package org.apache.datacommons.prepbuddy.smoothers

import scala.collection.mutable

class Weights(limit: Int) extends Serializable{
    def multiplyWith(queue: mutable.Queue[Double]): List[Double] = {
        weights.zip(queue).map((tuple) => tuple._1 * tuple._2)
    }

    private var weights: List[Double] = List()

    def get(index: Int): Double = weights(index)

    def add(value: Double): Unit = weights = weights :+ value

}
