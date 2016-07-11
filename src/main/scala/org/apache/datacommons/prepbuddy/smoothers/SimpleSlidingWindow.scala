package org.apache.datacommons.prepbuddy.smoothers

import scala.collection.mutable

class SimpleSlidingWindow(windowSize: Int) {
    protected var windowValues: mutable.Queue[Double] = mutable.Queue()

    def average: Double = {
        windowValues.sum / windowSize
    }

    def add(value: Double): Unit = {
        if(isFull){
            windowValues.dequeue()
        }
        windowValues += value
    }

    def isFull: Boolean = {
        windowValues.length == windowSize
    }


}
