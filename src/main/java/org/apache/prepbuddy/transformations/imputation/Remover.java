package org.apache.prepbuddy.transformations.imputation;

import org.apache.prepbuddy.exception.ColumnIndexOutOfBoundsException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Remover implements Serializable {
    List<Integer> columnIndexes = new ArrayList<Integer>();
    public Boolean hasFieldsValue(String[] column) {
        for (Integer columnIndex : columnIndexes) {
            if (columnIndex >= column.length || columnIndex < 0)
                throw new ColumnIndexOutOfBoundsException("No column found on index:: " + columnIndex);
            if (column[columnIndex] == null || column[columnIndex].trim().isEmpty())
                return false;
        }
        return true;
    }

    public void onColumn(int columnIndex) {
        columnIndexes.add(columnIndex);
    }
}
