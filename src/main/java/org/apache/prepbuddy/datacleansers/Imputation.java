package org.apache.prepbuddy.datacleansers;


public abstract class Imputation {

    public String[] apply(String[] row, int columnIndex) {
        String columnValue = row[columnIndex];
        if (columnValue == null || columnValue.trim().isEmpty())
            columnValue = handleMissingData(row);
        row[columnIndex] = columnValue;
        return row;
    }

    protected abstract String handleMissingData(String[] record);
}
