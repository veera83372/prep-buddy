package org.apache.prepbuddy.reformatters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ColumnTransformations implements Serializable {

    private int columnNumber;
    private List<TransformationFunction> transformationFunctions = new ArrayList<TransformationFunction>();

    public ColumnTransformations(int columnNumber) {
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

    public void setupImputation(ImputationTransformation transformation) {
        transformationFunctions.add(transformation);
    }
}
