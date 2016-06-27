package org.apache.prepbuddy.qualityanalyzers;

import org.apache.prepbuddy.utils.Range;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AnalysisPlan {

    private final List<Integer> columnIndexes;
    private List<String> missingValueHints;

    public AnalysisPlan(List<Integer> columnIndexes, List<String> missingValueHints) {
        this.columnIndexes = columnIndexes;
        this.missingValueHints = missingValueHints;
    }

    public AnalysisPlan(Range indexRange, List<String> missingValueHints) {
        this(indexRange.getContiniousValues(), missingValueHints);
    }

    public AnalysisPlan(int columnIndex) {
        this(Arrays.asList(columnIndex), Collections.EMPTY_LIST);
    }

    public List<Integer> columnIndexes() {
        return columnIndexes;
    }

    public List<String> missingHints() {
        return missingValueHints;
    }
}
