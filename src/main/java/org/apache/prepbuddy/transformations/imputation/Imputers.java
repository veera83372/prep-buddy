package org.apache.prepbuddy.transformations.imputation;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Imputers implements Serializable{
    private Map<Integer, HandlerFunction> handlerFunctions = new HashMap<>();

    public void add(int columnNumber, HandlerFunction function) {
        handlerFunctions.put(columnNumber, function);
    }

    public String[] handle(String[] columnValues) {
        for (Integer columnIndex : handlerFunctions.keySet()) {
            String value = columnValues[columnIndex];
            if (value == null || value.trim().isEmpty()) {
                Object imputedValue = handlerFunctions.get(columnIndex).handleMissingField(columnIndex);
                columnValues[columnIndex] = imputedValue.toString();
            }
        }
        return columnValues;
    }


    interface HandlerFunction extends Serializable {
        Object handleMissingField(int columnIndex);
    }
}
