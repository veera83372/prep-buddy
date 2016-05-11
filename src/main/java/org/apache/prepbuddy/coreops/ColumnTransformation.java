package org.apache.prepbuddy.coreops;

import org.apache.prepbuddy.datacleansers.Imputation;
import org.apache.prepbuddy.datacleansers.NominalToNumericTransformation;
import org.apache.prepbuddy.transformation.SplitByDelimiter;
import org.apache.prepbuddy.transformation.TransformationOperation;
import org.apache.prepbuddy.utils.DefaultValue;
import org.apache.prepbuddy.utils.Replacement;

import java.util.ArrayList;
import java.util.List;

public class ColumnTransformation implements TransformationOperation {

    private int columnNumber;
    private List<TransformationFunction> transformationFunctions = new ArrayList<TransformationFunction>();

    public ColumnTransformation(int columnNumber) {
        this.columnNumber = columnNumber;
    }

    public String[] apply(String[] row) {
        String[] transformedColumn = row;

        for (TransformationFunction rule : transformationFunctions) {
            transformedColumn = rule.apply(row,columnNumber);
        }
        return transformedColumn;
    }

    public void setupNominalToNumeric(final DefaultValue defaultt, final Replacement... pairs) {
        transformationFunctions.add(new NominalToNumericTransformation(defaultt, pairs));
    }

    public void setupImputation(Imputation transformation) {
        transformationFunctions.add(transformation);
    }

    public void splitBy(String splitter, boolean retainColumn) {
        transformationFunctions.add(new SplitByDelimiter(splitter, retainColumn));
    }
}
