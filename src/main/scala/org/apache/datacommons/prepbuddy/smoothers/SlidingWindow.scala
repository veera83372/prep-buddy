package org.apache.datacommons.prepbuddy.smoothers

import scala.collection.mutable

abstract class SlidingWindow(windowSize: Int) extends Serializable {
    protected var windowValues: mutable.Queue[Double] = mutable.Queue()

    def add(value: Double) {
        if (isFull) windowValues.dequeue()
        windowValues += value
    }

    def isFull: Boolean = windowValues.length == windowSize

    def average: Double
}
