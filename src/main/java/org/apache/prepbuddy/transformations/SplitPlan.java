package org.apache.prepbuddy.transformations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SplitPlan implements Serializable {
    private List<Integer> fieldLengths = null;
    private int columnIndex;
    private String separator;
    private Integer maxNumberOfSplit;
    private boolean retainColumn;

    public SplitPlan(int columnIndex, String separator, Integer maxNumberOfSplit, boolean retainColumn) {
        this.columnIndex = columnIndex;
        this.separator = separator;
        this.retainColumn = retainColumn;
        this.maxNumberOfSplit = maxNumberOfSplit;
    }

    public SplitPlan(int columnIndex, String separator, boolean retainColumn) {
        this(columnIndex, separator, null, retainColumn);
    }

    public SplitPlan(int columnIndex, List<Integer> fieldLengths, boolean retainColumn) {
        this.columnIndex = columnIndex;
        this.fieldLengths = fieldLengths;
        this.retainColumn = retainColumn;
    }

    public String[] splitColumn(String[] record) {
        String[] splittedColumn = splitColumnValue(record[columnIndex]);
        if (retainColumn)
            return arrangeRecordByKeepingColumn(splittedColumn, record);
        return arrangeRecordByRemovingColumn(splittedColumn, record);
    }

    private String[] splitColumnValue(String record) {
        if (fieldLengths != null)
            return splitColumnByLength(record);

        return splitColumnByDelimiter(record);
    }

    private String[] splitColumnByLength(String columnValue) {
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

    String[] splitColumnByDelimiter(String columnValue) {
        if (maxNumberOfSplit == null)
            return columnValue.split(separator);
        return columnValue.split(separator, maxNumberOfSplit);
    }

    private String[] arrangeRecordByKeepingColumn(String[] splittedColumn, String[] oldRecord) {
        int newRecordLength = splittedColumn.length + oldRecord.length;
        String[] resultHolder = new String[newRecordLength];

        int resultHolderIndex = 0;
        for (int index = 0; index < oldRecord.length; index++) {
            if (index == columnIndex && retainColumn) {
                resultHolder[resultHolderIndex++] = oldRecord[index];
                for (String value : splittedColumn)
                    resultHolder[resultHolderIndex++] = value;
            } else
                resultHolder[resultHolderIndex++] = oldRecord[index];
        }

        return resultHolder;
    }

    private String[] arrangeRecordByRemovingColumn(String[] splittedColumn, String[] oldRecord) {
        int newRecordLength = splittedColumn.length + oldRecord.length - 1;
        String[] resultHolder = new String[newRecordLength];

        int resultHolderIndex = 0;
        for (int index = 0; index < oldRecord.length; index++) {
            if (index == columnIndex)
                for (String value : splittedColumn)
                    resultHolder[resultHolderIndex++] = value;
            else
                resultHolder[resultHolderIndex++] = oldRecord[index];
        }

        return resultHolder;
    }
}
