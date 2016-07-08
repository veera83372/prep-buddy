package org.apache.datacommons.prepbuddy.exceptions

object ErrorMessages {
    val WEIGHTS_SUM_IS_NOT_EQUAL_TO_ONE: ErrorMessage = {
        new ErrorMessage("WEIGHTS_SUM_IS_NOT_EQUAL_TO_ONE", "To calculate weighted moving average weights sum should be up to one")
    }
}
