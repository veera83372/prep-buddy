package org.apache.prepbuddy.transformation;

import org.apache.prepbuddy.coreops.TransformationFunction;

public class ColumnSplitter implements TransformationFunction {
    private String separator;
    private Integer maxPartition;
    private boolean retainColumn;

    public ColumnSplitter(String separator, Integer maxPartition, boolean retainColumn) {
        this.separator = separator;
        this.maxPartition = maxPartition;
        this.retainColumn = retainColumn;
    }

    public ColumnSplitter(String separator, boolean retainColumn) {
        this(separator, null, retainColumn);
    }

    protected ColumnSplitter(boolean retainColumn) {
        this.retainColumn = retainColumn;
    }

    public String[] apply(String[] record, int columnIndex) {
        String[] splittedColumn = getSplittedRecord(record[columnIndex]);
//        if (retainColumn)
//            return arrangeRecordByRetainingColumn(splittedColumn, record, columnIndex);
        return arrangeRecordByRemovingColumn(splittedColumn, record, columnIndex);
    }

//    private String[] arrangeRecordByRetainingColumn(String[] splittedColumn, String[] oldRecord, int columnIndex) {
//        int newRecordLength = getNewRecordLength(splittedColumn, oldRecord);
//        String[] resultHolder = new String[newRecordLength];
//
//        int resultHolderIndex = 0;
//        for (int index = 0; index < oldRecord.length; index++) {
//            if (index == columnIndex+1)
//                for (String value : splittedColumn)
//                    resultHolder[resultHolderIndex++] = value;
//            else
//                resultHolder[resultHolderIndex++] = oldRecord[index];
//        }
//
//        return resultHolder;
//    }

    protected String[] getSplittedRecord(String columnValue) {
        if (maxPartition == null)
            return columnValue.split(separator);
        return columnValue.split(separator, maxPartition);
    }

    String[] arrangeRecordByRemovingColumn(String[] splittedColumn, String[] oldRecord, int columnIndex) {
        int newRecordLength = getNewRecordLength(splittedColumn, oldRecord);
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

    private int getNewRecordLength(String[] splittedColumn, String[] oldRecord) {
        int newRecordLength = splittedColumn.length + oldRecord.length;
        if (retainColumn)
            return newRecordLength;

        return newRecordLength - 1;
    }
}
