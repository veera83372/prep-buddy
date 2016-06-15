package org.apache.prepbuddy.transformers;

import scala.Serializable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MergePlan implements Serializable {
    private List<Integer> combinationOrder;
    private String separator;
    private boolean retainColumns;

    public MergePlan(List<Integer> combinationOrder, boolean retainColumns, String separator) {
        this.combinationOrder = combinationOrder;
        this.separator = separator;
        this.retainColumns = retainColumns;
    }

    public MergePlan(List<Integer> combinationOrder, boolean retainColumns) {
        this(combinationOrder, retainColumns, " ");
    }

    public String[] apply(String[] record) {
        String mergedValue = mergeColumns(record);
        if (retainColumns)
            return arrangeRecordByRetainingColumns(mergedValue, record);
        return arrangeRecordByRemovingColumns(mergedValue, record);
    }

    private String mergeColumns(String[] record) {
        String mergedRecord = "";
        for (Integer columnPosition : combinationOrder)
            mergedRecord += separator + record[columnPosition];

        return mergedRecord.substring(1);
    }

    private String[] arrangeRecordByRetainingColumns(String mergedValue, String[] oldRecord) {
        ArrayList<String> resultRecordHolder = new ArrayList<>(Arrays.asList(oldRecord));
        resultRecordHolder.add(mergedValue);
        return resultRecordHolder.toArray(new String[resultRecordHolder.size()]);
    }

    private String[] arrangeRecordByRemovingColumns(String mergedValue, String[] oldRecord) {
        ArrayList<String> resultRecordHolder = new ArrayList<>();

        for (int index = 0; index < oldRecord.length; index++) {
            if (combinationOrder.contains(index))
                continue;
            resultRecordHolder.add(oldRecord[index]);
        }
        resultRecordHolder.add(mergedValue);

        return resultRecordHolder.toArray(new String[resultRecordHolder.size()]);
    }
}
