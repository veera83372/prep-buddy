package org.apache.prepbuddy.transformations.imputation;

public class RowRecord {
    private final String[] columnValues;

    public RowRecord(final String[] columnValues) {
        this.columnValues = columnValues;
    }
    public String get(Integer columnIndex) {
        return columnValues[columnIndex];
    }
}
