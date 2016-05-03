package org.apache.prepbuddy.datacleansers;

import org.apache.prepbuddy.coreops.TransformationFunction;
import org.apache.prepbuddy.utils.RowRecord;

import java.io.Serializable;

public abstract class Imputation implements Serializable, TransformationFunction {

    @Override
    public String apply(String existingValue, String[] row) {
        if (existingValue == null || existingValue.trim().isEmpty())
            return handleMissingData(new RowRecord(row));
        return existingValue;
    }

    protected abstract String handleMissingData(RowRecord record);

}
