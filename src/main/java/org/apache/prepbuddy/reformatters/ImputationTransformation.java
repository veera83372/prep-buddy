package org.apache.prepbuddy.reformatters;

import java.io.Serializable;

public abstract class ImputationTransformation implements Serializable, TransformationFunction  {

    @Override
    public String apply(String existingValue, String[] row) {
        if (existingValue == null || existingValue.trim().isEmpty())
            return handleMissingData(new RowRecord(row));
        return existingValue;
    }

    protected abstract String handleMissingData(RowRecord record);

}
