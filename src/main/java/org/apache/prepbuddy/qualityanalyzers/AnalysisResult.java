package org.apache.prepbuddy.qualityanalyzers;

import org.apache.prepbuddy.utils.Range;

public class AnalysisResult {

    private int columnIndex;

    public AnalysisResult(int columnIndex) {

        this.columnIndex = columnIndex;
    }

    public DataType getDataType() {
        return null;
    }

    public Double percentageOfMissingValues() {
        return 0.0;
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
