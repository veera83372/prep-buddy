package org.apache.datacommons.prepbuddy.exceptions

object ErrorMessages {
    val WEIGHTS_SUM_IS_NOT_EQUAL_TO_ONE: ErrorMessage = {
        new ErrorMessage("WEIGHTS_SUM_IS_NOT_EQUAL_TO_ONE", "To calculate weighted moving average, weights sum should be up to one")
    }
    val WINDOW_SIZE_AND_WEIGHTS_SIZE_NOT_MATCHING: ErrorMessage = {
        new ErrorMessage("WINDOW_SIZE_AND_WEIGHTS_SIZE_NOT_MATCHING", "Window size and weighs size should be same")
    }
    val SIZE_LIMIT_IS_EXCEEDED: ErrorMessage = {
        new ErrorMessage("SIZE_LIMIT_IS_EXCEEDED", "Can not add value more than size limit")
    }
    val PROBABILITY_IS_NOT_IN_RANGE: ErrorMessage = {
        new ErrorMessage("PROBABILITY_IS_NOT_IN_RANGE", "Probability can not be less than zero or greater than 1")
    }
}
