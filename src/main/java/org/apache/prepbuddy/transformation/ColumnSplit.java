package org.apache.prepbuddy.transformation;

import java.io.Serializable;

public class ColumnSplit implements Serializable {
    private int columnIndex;
    private String separator;

    public ColumnSplit(int columnIndex, String separator) {
        this.columnIndex = columnIndex;
        this.separator = separator;
    }

    public String[] apply(String[] record) {
        String[] splittedColumn = record[columnIndex].split(separator);
        return arrangeRecord(splittedColumn, record);
    }

    private String[] arrangeRecord(String[] splittedColumn, String[] oldRecord) {
        int newRecordLength = splittedColumn.length + oldRecord.length - 1;
        String[] resultHolder = new String[newRecordLength];

        int resultHolderIndex = 0;
        for (int index = 0; index < oldRecord.length; index++){
            if(index == columnIndex)
                for (String value : splittedColumn)
                    resultHolder[resultHolderIndex++] = value;

            else
                resultHolder[resultHolderIndex++] = oldRecord[index];
        }

        return resultHolder;
    }
}
