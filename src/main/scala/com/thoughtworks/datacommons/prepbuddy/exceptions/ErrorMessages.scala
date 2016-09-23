package com.thoughtworks.datacommons.prepbuddy.exceptions

object ErrorMessages {

    val COLUMN_VALUES_ARE_NOT_NUMERIC: ErrorMessage = {
        val message = "Values of column are not numeric"
        val key = "COLUMN_VALUES_ARE_NOT_NUMERIC"
        new ErrorMessage(key, message)
    }

    val WEIGHTS_SUM_IS_NOT_EQUAL_TO_ONE: ErrorMessage = {
        val message = "To calculate weighted moving average weights sum should be up to one."
        val key = "WEIGHTS_SUM_IS_NOT_EQUAL_TO_ONE"
        new ErrorMessage(key, message)
    }

    val WINDOW_SIZE_AND_WEIGHTS_SIZE_NOT_MATCHING: ErrorMessage = {
        val message = "Window size and weighs size should be same."
        val key = "WINDOW_SIZE_AND_WEIGHTS_SIZE_NOT_MATCHING"
        new ErrorMessage(key, message)
    }

    val SIZE_LIMIT_IS_EXCEEDED: ErrorMessage = {
        val key = "SIZE_LIMIT_IS_EXCEEDED"
        val message = "Can not add value more than size limit."
        new ErrorMessage(key, message)
    }

    val COLUMN_INDEX_OUT_OF_BOUND: ErrorMessage = {
        val key = "COLUMN_INDEX_OUT_OF_BOUND"
        val message = "Column index is out of bound."
        new ErrorMessage(key, message)
    }

    val NEGATIVE_COLUMN_INDEX: ErrorMessage = {
        val key = "NEGATIVE_COLUMN_INDEX"
        val message = "Column index can not be negative."
        new ErrorMessage(key, message)
    }

    val PROBABILITY_IS_NOT_IN_RANGE: ErrorMessage = {
        val key = "PROBABILITY_IS_NOT_IN_RANGE"
        val message = "Probability can not be less than zero or greater than 1"
        new ErrorMessage(key, message)
    }

    val NUMBER_OF_COLUMN_DID_NOT_MATCHED: ErrorMessage = {
        val key = "NUMBER_OF_COLUMN_DID_NOT_MATCHED"
        val message = "Number of fields must be same in the expected schema"
        new ErrorMessage(key, message)
    }

    var REQUIREMENT_NOT_MATCHED: ErrorMessage = {
        val message = "Requirement did not matched"
        val key = "REQUIREMENT_NOT_MATCHED"
        new ErrorMessage(key, message)
    }

    var SCHEMA_NOT_SET: ErrorMessage = {
        val messages = "Schema is not set"
        val key = "SCHEMA_NOT_SET"
        new ErrorMessage(key, messages)
    }

    val NO_SUCH_COLUMN_NAME_FOUND: ErrorMessage = {
        val messages = "No such column name is found in the schema"
        val key = "NO_SUCH_COLUMN_NAME_FOUND"
        new ErrorMessage(key, messages)
    }

    val INVALID_COLUMN_REFERENCE_FOUND: ErrorMessage = {
        val messages = "Invalid column reference found in the schema"
        val key = "INVALID_COLUMN_REFERENCE_FOUND"
        new ErrorMessage(key, messages)
    }
}
