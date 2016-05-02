package org.apache.prepbuddy;

import org.apache.prepbuddy.reformatters.ColumnTransformations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DatasetTransformations implements Serializable{
    List<ColumnTransformations> allColumnRules = new ArrayList<ColumnTransformations>();

    public void addColumnTransformations(ColumnTransformations columnRules) {
        allColumnRules.add(columnRules);
    }


    public String[] apply(String[] untransformedRow) {
        String[] transformedRow = untransformedRow;

        for (ColumnTransformations columnRules : allColumnRules) {
            transformedRow = columnRules.applyRules(transformedRow);
        }
        return transformedRow;
    }
}
