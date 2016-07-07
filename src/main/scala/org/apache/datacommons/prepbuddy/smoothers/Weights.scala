package org.apache.datacommons.prepbuddy.smoothers

class Weights(limit: Int) extends Serializable{
    private var weights: List[Double] = List()

    def get(index: Int): Double = weights(index)

    def add(value: Double): Unit = weights = weights :+ value

}
