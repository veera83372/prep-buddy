package org.apache.datacommons.prepbuddy

import org.apache.datacommons.prepbuddy.exceptions.ApplicationException
import org.apache.datacommons.prepbuddy.smoothers.{WeightedSlidingWindow, Weights}

class ExceptionTest extends SparkTestCase {
    test("Weighted moving average should not be creatable when weights and window size is not equal") {
        val weights: Weights = new Weights(3)
        val thrown = intercept[ApplicationException] {
            new WeightedSlidingWindow(1, weights)
        }
        assert(thrown.getMessage == "Window size and weighs size should be same.")
    }

    test("weights add should throw exception when sum of weights is not equal to one") {
        val weights: Weights = new Weights(3)
        weights.add(0.166)
        weights.add(0.333)
        weights.add(0.5)

        val otherWeights: Weights = new Weights(2)
        otherWeights.add(0.777)
        val thrown = intercept[ApplicationException] {
            otherWeights.add(0.333)
        }
        assert(thrown.getMessage == "To calculate weighted moving average weights sum should be up to one.")
    }

    test("weights should throw exception if size is exceeded") {
        val weights: Weights = new Weights(2)
        weights.add(0.5)
        weights.add(0.5)
        val thrown = intercept[ApplicationException] {
            weights.add(0.3)
        }
        assert(thrown.getMessage == "Can not add value more than size limit.")
    }
}
