package org.apache.prepbuddy.qualityanalyzers;

import org.apache.prepbuddy.utils.Range;

import java.util.Map;

public class AnalysisResult {

    private int columnIndex;
    private DataType dataType;
    private double totalNumberOfRows;
    private Map<Integer, Integer> missingDataReport; //ColumnIndex, Count

    public AnalysisResult(int columnIndex, DataType dataType, long totalNumberOfRows, Map<Integer, Integer> missingDataReport) {
        this.columnIndex = columnIndex;
        this.dataType = dataType;
        this.totalNumberOfRows = totalNumberOfRows;
        this.missingDataReport = missingDataReport;
    }

    public DataType dataType() {
        return dataType;
    }

    public Double percentageOfMissingValues() {
        Integer missingDataCount = missingDataReport.get(columnIndex);
        double percentage = (missingDataCount / totalNumberOfRows) * 100;
        return percentage;
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
