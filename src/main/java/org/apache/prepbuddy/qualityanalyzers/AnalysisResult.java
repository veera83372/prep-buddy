package org.apache.prepbuddy.qualityanalyzers;

import org.apache.prepbuddy.utils.Range;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AnalysisResult {


    private final Map<Integer, DataType> dataTypeReport; //ColumnIndex, Count
    private Map<Integer, Integer> missingValueReport; //ColumnIndex, Count
    private double totalNumberOfRows;

    public AnalysisResult(long totalNumberOfRows, Map<Integer, DataType> dataTypeReport, Map<Integer, Integer> missingValueReport) {
        this.totalNumberOfRows = totalNumberOfRows;
        this.dataTypeReport = dataTypeReport;
        this.missingValueReport = missingValueReport;
    }

    public Map<Integer, DataType> dataType() {
        return dataTypeReport;
    }

    public Map<Integer, Double> percentageOfMissingValues() {
        Set<Integer> columnIndexes = missingValueReport.keySet();
        HashMap<Integer, Double> missingPercentage = new HashMap<>();
        for (Integer columnIndex : columnIndexes) {
            Integer numberOfMissingValueAtColumn = missingValueReport.get(columnIndex);
            double percentage = getPercentage(numberOfMissingValueAtColumn, totalNumberOfRows);
            missingPercentage.put(columnIndex, percentage);
        }
        return missingPercentage;
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

    public DataType dataTypeAt(int columnIndex) throws IllegalAccessException {
        if (!dataTypeReport.containsKey(columnIndex))
            throw new IllegalAccessException("No report found for index " + columnIndex);
        return dataTypeReport.get(columnIndex);
    }

    public Double percentageOfMissingValueAt(int columnIndex) throws IllegalAccessException {
        if (!missingValueReport.containsKey(columnIndex))
            throw new IllegalAccessException("No report found for index " + columnIndex);
        return getPercentage(missingValueReport.get(columnIndex), totalNumberOfRows);
    }
}
