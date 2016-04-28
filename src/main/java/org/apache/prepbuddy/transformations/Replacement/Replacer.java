package org.apache.prepbuddy.transformations.Replacement;

import java.io.Serializable;
import java.util.HashMap;

public class Replacer implements Serializable {
    private HashMap<Integer, ReplaceHandler> replaceHandlerHashMap = new HashMap<Integer, ReplaceHandler>();

    public void add(int columnIndex, ReplaceHandler<String, String> replaceHandler) {
        replaceHandlerHashMap.put(columnIndex, replaceHandler);
    }

    public String[] replaceValue(String[] row) {
        for (Integer columnIndex : replaceHandlerHashMap.keySet()) {
            row[columnIndex] = (String) replaceHandlerHashMap.get(columnIndex).replace(row[columnIndex]);
        }
        return row;
    }


    public interface ReplaceHandler<T,R> extends Serializable {
        R replace(T value);
    }
}
