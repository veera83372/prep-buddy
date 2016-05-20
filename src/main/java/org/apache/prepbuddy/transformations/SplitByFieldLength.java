package org.apache.prepbuddy.transformations;

import java.util.ArrayList;
import java.util.List;

public class SplitByFieldLength extends SplitPlan {
    private final List<Integer> fieldLengths;

    public SplitByFieldLength(List<Integer> fieldLengths, boolean retainColumn) {
        super(retainColumn);
        this.fieldLengths = fieldLengths;
    }

    @Override
    String[] splitColumn(String columnValue) {
        int startingIndex = 0;
        ArrayList<String> splittedColumn = new ArrayList<>();

        for (Integer fieldLength : fieldLengths) {
            int endingIndex = startingIndex + fieldLength;
            String value = columnValue.substring(startingIndex, endingIndex);
            splittedColumn.add(value);
            startingIndex += fieldLength;
        }

        String[] resultArray = new String[splittedColumn.size()];
        return splittedColumn.toArray(resultArray);
    }
}
