package org.apache.datacommons.prepbuddy.smoothers

import org.apache.datacommons.prepbuddy.exceptions.{ApplicationException, ErrorMessages}

class WeightedSlidingWindow(windowSize: Int, weights: Weights) extends SlidingWindow(windowSize) {
    if (windowSize != weights.size){
        throw new ApplicationException(ErrorMessages.WINDOW_SIZE_AND_WEIGHTS_SIZE_NOT_MATCHING)
    }

    def average: Double = weights.multiplyWith(windowValues).sum
}
