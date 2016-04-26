package org.apache.prepbuddy.transformations.imputation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Remover implements Serializable {
    private List<Integer> columns = new ArrayList<>();
    public void onColumn(int columnIndex) {
            columns.add(columnIndex);
    }

    public Boolean hasFieldsValue(String row) {
        String[] columnValues = row.split(",");
        for (Integer columnIndex : columns) {
            if(columnValues[columnIndex] == null || columnValues[columnIndex].trim().isEmpty())
                return false;
        }
        return true;
    }
}
