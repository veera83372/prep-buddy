package org.apache.prepbuddy.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PivotTable<T> implements Serializable {
    private HashMap<String, Map<String, T>> lookupTable = new HashMap<>();
    private T defaultValue;

    public PivotTable(T defaultValue) {
        this.defaultValue = defaultValue;
    }

    public void addEntry(String rowKey, String columnKey, T value) {
        if (!lookupTable.containsKey(rowKey))
            lookupTable.put(rowKey, new HashMap<String, T>());
        lookupTable.get(rowKey).put(columnKey, value);
    }

    public T valueAt(String rowKey, String columnKey) {
        Map<String, T> row = lookupTable.get(rowKey);
        T columnValue = row.get(columnKey);
        return columnValue == null ? defaultValue : columnValue;
    }

    public PivotTable transform(TransformationFunction transformation) {
        PivotTable pivotTable = new PivotTable<>(transformation.defaultValue());
        Set<String> rowKeys = lookupTable.keySet();
        for (String rowKey : rowKeys) {
            Map<String, T> columns = lookupTable.get(rowKey);
            Set<String> columnKeys = columns.keySet();
            for (String columnKey : columnKeys) {
                T value = columns.get(columnKey);
                pivotTable.addEntry(rowKey, columnKey, transformation.transform(value));
            }
        }
        return pivotTable;
    }

}
