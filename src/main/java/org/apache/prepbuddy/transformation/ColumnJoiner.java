package org.apache.prepbuddy.transformation;

import java.util.List;

public class ColumnJoiner implements TransformationOperation {
    private List<Integer> combinationOrder;
    private String separator;
    private boolean retainColumns;

    public ColumnJoiner(List<Integer> combinationOrder, String separator, boolean retainColumns) {
        this.combinationOrder = combinationOrder;
        this.separator = separator;
        this.retainColumns = retainColumns;
    }

    public ColumnJoiner(List<Integer> combinationOrder, boolean retainColumns) {
        this(combinationOrder, " ", retainColumns);
    }

    public String[] apply(String[] record) {
        String mergedValue = margeColumns(record);
        if (retainColumns)
            return arrangeRecordByRetainingColumns(mergedValue, record);
        return arrangeRecordByRemovingColumns(mergedValue, record);
    }

    private String[] arrangeRecordByRetainingColumns(String mergedValue, String[] oldRecord) {
        int newRecordLength = oldRecord.length + 1;
        String[] resultRecordHolder = new String[newRecordLength];

        System.arraycopy(oldRecord, 0, resultRecordHolder, 0, oldRecord.length);

        resultRecordHolder[newRecordLength - 1] = mergedValue;
        return resultRecordHolder;
    }

    private String margeColumns(String[] record) {
        String mergedRecord = "";
        for (Integer columnPosition : combinationOrder)
            mergedRecord += separator + record[columnPosition];

        return mergedRecord.substring(1);
    }

    private String[] arrangeRecordByRemovingColumns(String mergedValue, String[] oldRecord) {
        int newRecordLength = oldRecord.length - combinationOrder.size() + 1;

        String[] resultRecordHolder = new String[newRecordLength];
        int resultRecordIndex = 0;

        Integer mergedValueIndex = mergedValueIndex();
        resultRecordHolder[mergedValueIndex] = mergedValue;

        for (int index = 0; index < oldRecord.length; index++) {
            if (resultRecordIndex == mergedValueIndex)
                resultRecordIndex++;

            if (!combinationOrder.contains(index))
                resultRecordHolder[resultRecordIndex++] = oldRecord[index];
        }
        return resultRecordHolder;
    }

    private Integer mergedValueIndex() {
        Integer destinationIndex = combinationOrder.get(0);
        int count = 0;
        for (Integer index : combinationOrder) {
            if (index < destinationIndex)
                count++;
        }
        return destinationIndex - count;
    }
}
