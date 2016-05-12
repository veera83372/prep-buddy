package org.apache.prepbuddy.utils;

public class RowRecord {
    private String[] columnValues;

    public RowRecord(final String[] columnValues) {
        this.columnValues = columnValues;
    }

    public String get(int columnIndex) {
        return columnValues[columnIndex];
    }
}
