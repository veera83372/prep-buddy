package org.apache.datacommons.prepbuddy.smoothers

import scala.collection.mutable

class WeightedSlidingWindow(windowSize: Int, weights: Weights) {
    def average: Double = queue.sum

    private var queue: mutable.Queue[Double] = mutable.Queue()

    def isFull: Boolean = queue.length == windowSize

    def add(value: Int): Unit = {
        if(isFull) queue.dequeue()
        val weightValue: Double = weights.get(queue.length)
        queue += value * weightValue
    }


}
