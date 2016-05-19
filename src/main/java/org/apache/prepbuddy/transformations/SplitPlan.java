package org.apache.prepbuddy.transformations;

import java.io.Serializable;

public abstract class SplitPlan implements Serializable {
    private boolean retainColumn;

    public SplitPlan(boolean retainColumn) {
        this.retainColumn = retainColumn;
    }

    public String[] apply(String[] record, int columnIndex) {
        String[] splittedColumn = splitColumn(record[columnIndex]);
        if (retainColumn)
            return arrangeRecordByKeepingColumn(splittedColumn, record, columnIndex);
        return arrangeRecordByRemovingColumn(splittedColumn, record, columnIndex);
    }

    abstract String[] splitColumn(String columnValue);

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
