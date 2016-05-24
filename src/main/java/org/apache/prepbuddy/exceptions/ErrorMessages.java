package org.apache.prepbuddy.exceptions;

public class ErrorMessages {
    public static final ErrorMessage COLUMN_INDEX_OUT_OF_BOUND = new ErrorMessage("COLUMN_INDEX_OUT_OF_BOUND", "");
    public static final ErrorMessage COLUMN_VALUES_ARE_NOT_NUMERIC = new ErrorMessage("COLUMN_VALUES_ARE_NOT_NUMERIC", "Values of column are not numaric");
    public static final ErrorMessage PROBABILITY_IS_NOT_IN_RANGE = new ErrorMessage("PROBABILITY_IS_NOT_IN_RANGE", "Probability can not be less than zero or greater than 1");
}
