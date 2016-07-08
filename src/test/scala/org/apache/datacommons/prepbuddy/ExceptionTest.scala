package org.apache.datacommons.prepbuddy

import org.apache.datacommons.prepbuddy.exceptions.ApplicationException
import org.apache.datacommons.prepbuddy.smoothers.{WeightedMovingAverageMethod, Weights}

class ExceptionTest extends SparkTestCase{
    test("Weighted moving average should not creatable when weights sze and window size is not equal") {
        val weights: Weights = new Weights(3)
        val thrown = intercept[ApplicationException] {
            new WeightedMovingAverageMethod(1, weights)
        }
        assert(thrown.getMessage == "Window size and weighs size should be same")
    }


}
