package org.apache.prepbuddy.coreops;

import org.apache.prepbuddy.utils.DefaultValue;
import org.apache.prepbuddy.datacleansers.Imputation;
import org.apache.prepbuddy.datacleansers.NominalToNumericTransformation;
import org.apache.prepbuddy.utils.Replacement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ColumnTransformation implements Serializable {

    private int columnNumber;
    private List<TransformationFunction> transformationFunctions = new ArrayList<TransformationFunction>();

    public ColumnTransformation(int columnNumber) {
        this.columnNumber = columnNumber;
    }

    public String[] applyRules(String[] row) {
        String transformedColumn = row[columnNumber];

        for (TransformationFunction rule : transformationFunctions) {
            transformedColumn = rule.apply(transformedColumn,row);
        }
        row[columnNumber] = transformedColumn;
        return row;
    }

    public void setupNominalToNumeric(final DefaultValue defaultt, final Replacement... pairs) {
        transformationFunctions.add(new NominalToNumericTransformation(defaultt, pairs));
    }

    public void setupImputation(Imputation transformation) {
        transformationFunctions.add(transformation);
    }
}
