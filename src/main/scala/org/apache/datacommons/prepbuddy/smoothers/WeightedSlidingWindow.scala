package org.apache.datacommons.prepbuddy.smoothers

import org.apache.datacommons.prepbuddy.exceptions.{ErrorMessages, ApplicationException}

import scala.collection.mutable

class WeightedSlidingWindow(windowSize: Int, weights: Weights) extends Serializable{
    if (windowSize != weights.size){
        throw new ApplicationException(ErrorMessages.WINDOW_SIZE_AND_WEIGHTS_SIZE_NOT_MATCHING)
    }
    private var queue: mutable.Queue[Double] = mutable.Queue()

    def average: Double = weights.multiplyWith(queue).sum

    def add(value: Double): Unit = {
        if (isFull) queue.dequeue()
        queue += value
    }

    def isFull: Boolean = queue.length == windowSize


}
