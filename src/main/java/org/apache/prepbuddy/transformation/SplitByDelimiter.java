package org.apache.prepbuddy.transformation;

import org.apache.prepbuddy.coreops.TransformationFunction;

public class SplitByDelimiter implements TransformationFunction {
    private String separator;
    private Integer maxPartition;
    private boolean retainColumn;

    public SplitByDelimiter(String separator, Integer maxPartition, boolean retainColumn) {
        this.separator = separator;
        this.maxPartition = maxPartition;
        this.retainColumn = retainColumn;
    }

    public SplitByDelimiter(String separator, boolean retainColumn) {
        this(separator, null, retainColumn);
    }

    protected SplitByDelimiter(boolean retainColumn) {
        this.retainColumn = retainColumn;
    }

    public String[] apply(String[] record, int columnIndex) {
        String[] splittedColumn = getSplittedRecord(record[columnIndex]);
        if (retainColumn)
            return arrangeRecordByKeepingColumn(splittedColumn, record, columnIndex);
        return arrangeRecordByRemovingColumn(splittedColumn, record, columnIndex);
    }

    protected String[] getSplittedRecord(String columnValue) {
        if (maxPartition == null)
            return columnValue.split(separator);
        return columnValue.split(separator, maxPartition);
    }

    private String[] arrangeRecordByKeepingColumn(String[] splittedColumn, String[] oldRecord, int columnIndex) {
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

    String[] arrangeRecordByRemovingColumn(String[] splittedColumn, String[] oldRecord, int columnIndex) {
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
