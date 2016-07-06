package org.apache.datacommons.prepbuddy.smoothers

class SimpleSlidingWindow(windowSize: Int) {
    def average(): Double = {
        windowValues.sum / windowSize
    }

    protected var windowValues: List[Double] = List()

    def isFull(): Boolean = {
        windowValues.length == windowSize
    }


    def add(value: Double): Unit = {
        if(isFull){
            windowValues = windowValues.drop(1)
        }
        windowValues = windowValues :+ value
    }


}
