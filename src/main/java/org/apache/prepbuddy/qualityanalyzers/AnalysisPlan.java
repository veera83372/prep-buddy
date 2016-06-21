package org.apache.prepbuddy.qualityanalyzers;

public class AnalysisPlan {

    private int columnIndex;

    public AnalysisPlan(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    public int columnIndex() {
        return columnIndex;
    }
}
