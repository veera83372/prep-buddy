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
        String margedValue = margeColumns(record);
        if (retainColumns)
            return arrangeRecordByRetainingColumns(margedValue, record);
        return arrangeRecordByRemovingColumns(margedValue, record);
    }

    private String[] arrangeRecordByRetainingColumns(String margedValue, String[] oldRecord) {
        int newRecordLength = oldRecord.length + 1;
        String[] resultRecordHolder = new String[newRecordLength];

        for (int index = 0; index < oldRecord.length; index++)
            resultRecordHolder[index] = oldRecord[index];

        resultRecordHolder[newRecordLength - 1] = margedValue;
        return resultRecordHolder;
    }

    private String margeColumns(String[] record) {
        String margedRecord = "";
        for (Integer columnPosition : combinationOrder)
            margedRecord += separator + record[columnPosition];

        return margedRecord.substring(1);
    }

    private String[] arrangeRecordByRemovingColumns(String margedValue, String[] oldRecord) {
        int newRecordLength = oldRecord.length - combinationOrder.size() + 1;

        String[] resultRecordHolder = new String[newRecordLength];
        int resultRecordIndex = 0;

        Integer margedValueIndex = margedValueIndex();
        resultRecordHolder[margedValueIndex] = margedValue;

        for (int index = 0; index < oldRecord.length; index++) {
            if (resultRecordIndex == margedValueIndex)
                resultRecordIndex++;

            if (!combinationOrder.contains(index))
                resultRecordHolder[resultRecordIndex++] = oldRecord[index];
        }
        return resultRecordHolder;
    }

    private Integer margedValueIndex() {
        Integer destinationIndex = combinationOrder.get(0);
        int count = 0;
        for (Integer index : combinationOrder) {
            if (index < destinationIndex)
                count++;
        }
        return destinationIndex - count;
    }
}
