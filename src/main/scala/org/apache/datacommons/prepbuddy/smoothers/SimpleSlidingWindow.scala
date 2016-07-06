package org.apache.datacommons.prepbuddy.smoothers

import scala.collection.mutable

class SimpleSlidingWindow(windowSize: Int) {
    def average: Double = {
        windowValues.sum / windowSize
    }

    protected var windowValues: mutable.Queue[Double] = mutable.Queue()

    def isFull(): Boolean = {
        windowValues.length == windowSize
    }


    def add(value: Double): Unit = {
        if(isFull){
            windowValues.dequeue()
        }
        windowValues += value
    }


}
