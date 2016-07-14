package org.apache.datacommons.prepbuddy.smoothers

class SimpleSlidingWindow(windowSize: Int) extends SlidingWindow(windowSize) {
    def average: Double = windowValues.sum / windowSize
}
