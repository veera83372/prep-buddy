package org.apache.prepbuddy.transformation;

import java.util.List;

public class ColumnJoiner implements TransformationOperation {
    private List<Integer> combinationOrder;
    private String separator;

    public ColumnJoiner(List<Integer> combinationOrder) {
        this(combinationOrder, " ");
    }

    public ColumnJoiner(List<Integer> combinationOrder, String separator) {
        this.combinationOrder = combinationOrder;
        this.separator = separator;
    }

    public String[] apply(String[] record) {
        String margedValue = margeColumns(record);
        return arrangeRecord(margedValue, record);
    }

    private String margeColumns(String[] record) {
        String margedRecord = "";
        for (Integer columnPosition : combinationOrder)
            margedRecord += separator + record[columnPosition];

        return margedRecord.substring(1);
    }

    private String[] arrangeRecord(String margedValue, String[] oldRecord) {
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
