package org.apache.prepbuddy.datacleansers;

import org.apache.prepbuddy.coreops.TransformationFunction;

public abstract class Imputation implements TransformationFunction {

    @Override
    public String apply(String existingValue, String[] row) {
        if (existingValue == null || existingValue.trim().isEmpty())
            return handleMissingData(row);
        return existingValue;
    }

    protected abstract String handleMissingData(String[] record);
}
