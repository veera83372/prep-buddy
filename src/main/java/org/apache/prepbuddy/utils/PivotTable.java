package org.apache.prepbuddy.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class PivotTable<T> implements Serializable {
    private HashMap<String, Map<String, T>> lookupTable = new HashMap<>();

    public void addEntry(String rowKey, String columnKey, T value) {
        if (!lookupTable.containsKey(rowKey))
            lookupTable.put(rowKey, new HashMap<String, T>());
        lookupTable.get(rowKey).put(columnKey, value);
    }

    public T valueAt(String rowKey, String columnKey) {
        return lookupTable.get(rowKey).get(columnKey);
    }
}
