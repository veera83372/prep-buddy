package org.apache.datacommons.prepbuddy.smoothers

import scala.collection.mutable

class WeightedSlidingWindow(windowSize: Int, weights: Weights) {
    def average: Double = weights.multiplyWith(queue).sum

    private var queue: mutable.Queue[Double] = mutable.Queue()

    def isFull: Boolean = queue.length == windowSize

    def add(value: Double): Unit = {
        if(isFull) queue.dequeue()
        queue += value
    }


}
