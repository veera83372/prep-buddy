package org.apache.prepbuddy.datacleansers;

import org.apache.prepbuddy.coreops.TransformationFunction;

public abstract class Imputation implements TransformationFunction {

    @Override
    public String[] apply(String[] row, int columnIndex) {
        String columnValue = row[columnIndex];
        if (columnValue == null || columnValue.trim().isEmpty())
            columnValue = handleMissingData(row);
        row[columnIndex] = columnValue;
        return row;
    }

    protected abstract String handleMissingData(String[] record);
}
