package org.apache.datacommons.prepbuddy.smoothers

import scala.collection.mutable

class WeightedSlidingWindow(windowSize: Int, weights: Weights) {
    private var queue: mutable.Queue[Double] = mutable.Queue()

    def average: Double = weights.multiplyWith(queue).sum

    def add(value: Double): Unit = {
        if (isFull) queue.dequeue()
        queue += value
    }

    def isFull: Boolean = queue.length == windowSize


}
