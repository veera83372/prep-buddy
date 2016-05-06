package org.apache.prepbuddy.transformation;

public class ColumnSplitter implements TransformationOperation {
    private int columnIndex;
    private String separator;
    private Integer maxPartition;

    public ColumnSplitter(int columnIndex, String separator, Integer maxPartition) {
        this.columnIndex = columnIndex;
        this.separator = separator;
        this.maxPartition = maxPartition;
    }

    protected ColumnSplitter(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    public ColumnSplitter(int columnIndex, String separator) {
        this(columnIndex, separator, null);
    }

    public String[] apply(String[] record) {
        String[] splittedColumn = getSplittedRecord(record[columnIndex]);
        return arrangeRecord(splittedColumn, record);
    }

    protected String[] getSplittedRecord(String columnValue) {
        if(maxPartition == null)
            return columnValue.split(separator);
        return columnValue.split(separator,maxPartition);
    }

    String[] arrangeRecord(String[] splittedColumn, String[] oldRecord) {
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
