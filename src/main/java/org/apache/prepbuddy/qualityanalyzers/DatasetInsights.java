package org.apache.prepbuddy.qualityanalyzers;

import org.apache.prepbuddy.utils.Range;

import java.util.Map;

public class DatasetInsights {

    private double totalNumberOfRows;
    private Map<Integer, ColumnInsight> columnInsights; //ColumnIndex, ColumnInsight

    public DatasetInsights(long totalNumberOfRows, Map<Integer, ColumnInsight> columnInsights) {
        this.totalNumberOfRows = totalNumberOfRows;
        this.columnInsights = columnInsights;
    }

    public DataType dataType(int columnIndex) throws IllegalAccessException {
        if (!columnInsights.containsKey(columnIndex))
            throw new IllegalAccessException("No report found for index " + columnIndex);
        return columnInsights.get(columnIndex).dataType();
    }

    public Double percentageOfMissingValue(int columnIndex) throws IllegalAccessException {
        if (!columnInsights.containsKey(columnIndex))
            throw new IllegalAccessException("No report found for index " + columnIndex);
        double amountOfMissingValue = columnInsights.get(columnIndex).amountOfMissingValue();
        return getPercentage(amountOfMissingValue, totalNumberOfRows);
    }

    private double getPercentage(double relativeNumber, double wholeNumber) {
        return (relativeNumber / wholeNumber) * 100;
    }

    public Double percentageOfInconsistentValues() {
        return 0.0;
    }

    public Double percentageOfDuplicateValues() {
        return 0.0;
    }

    public Range rangeOfValues() {
        return null;
    }

    public DataShape shapeOfData() {

        return null;
    }

    public Outliers outliers() {
        return null;
    }

    public Double skewness() {
        return null;
    }

    public Double kurtosis() {
        return null;
    }
}
