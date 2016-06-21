package org.apache.prepbuddy.qualityanalyzers;

import java.util.Collections;
import java.util.List;

public class AnalysisPlan {

    private int columnIndex;
    private List<String> missingValueHints;

    public AnalysisPlan(int columnIndex, List<String> missingValueHints) {
        this.columnIndex = columnIndex;
        this.missingValueHints = missingValueHints;
    }

    public AnalysisPlan(int columnIndex) {
        this(columnIndex, Collections.EMPTY_LIST);
    }

    public int columnIndex() {
        return columnIndex;
    }

    public List<String> missingHints() {
        return missingValueHints;
    }
}
