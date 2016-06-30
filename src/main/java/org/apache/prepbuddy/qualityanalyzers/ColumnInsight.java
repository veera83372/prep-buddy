package org.apache.prepbuddy.qualityanalyzers;

public class ColumnInsight {
    private final int columnIndex;
    private final DataType dataType;
    private final Integer countOfMissingValues;

    public ColumnInsight(int columnIndex, DataType dataType, Integer countOfMissingValues) {
        this.columnIndex = columnIndex;
        this.dataType = dataType;
        this.countOfMissingValues = countOfMissingValues;
    }

    public DataType dataType() {
        return dataType;
    }

    public Integer countOfMissingValues() {
        return countOfMissingValues;
    }
}
