package org.apache.prepbuddy.qualityanalyzers;

public class ColumnInsight {
    private final int columnIndex;
    private final DataType dataType;
    private final Integer amountOfMissingValue;

    public ColumnInsight(int columnIndex, DataType dataType, Integer amountOfMissingValue) {
        this.columnIndex = columnIndex;
        this.dataType = dataType;
        this.amountOfMissingValue = amountOfMissingValue;
    }

    public DataType dataType() {
        return dataType;
    }

    public Integer amountOfMissingValue() {
        return amountOfMissingValue;
    }
}
