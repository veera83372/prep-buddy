package org.apache.prepbuddy.reformatters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ColumnTransformations implements Serializable {

    private int columnNumber;
    private List<TransformerFunction> transformerFunctions = new ArrayList<TransformerFunction>();

    public ColumnTransformations(int columnNumber) {
        this.columnNumber = columnNumber;
    }

    public String[] applyRules(String[] row) {
        String transformedColumn = row[columnNumber];

        for (TransformerFunction rule : transformerFunctions) {
            transformedColumn = rule.apply(transformedColumn);
        }
        row[columnNumber] = transformedColumn;
        return row;
    }

//    public void addRule(TransformerFunction transformerFunction) {
//        transformerFunctions.add(transformerFunction);
//    }

    public void setupNominalToNumeric(final DefaultValue defaultt, final Replacement... pairs) {
        transformerFunctions.add(new NominalToNumericTransformation(defaultt, pairs));
    }

}
