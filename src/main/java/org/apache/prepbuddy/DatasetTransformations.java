package org.apache.prepbuddy;

import org.apache.prepbuddy.reformatters.ColumnTransformations;
import org.apache.prepbuddy.reformatters.RowPredicate;
import org.apache.prepbuddy.reformatters.RowRemovalTransformation;
import org.apache.prepbuddy.reformatters.RowTransformation;
import org.apache.prepbuddy.transformations.deduplication.DeduplicationTransformation;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DatasetTransformations implements Serializable {
    List<ColumnTransformations> allColumnRules = new ArrayList<ColumnTransformations>();
    List<RowTransformation> allRowRules = new ArrayList<>();

    public void addColumnTransformations(ColumnTransformations columnRules) {
        allColumnRules.add(columnRules);
    }


    public String[] applyColumnTransforms(String[] untransformedRow) {
        String[] transformedRow = untransformedRow;

        for (ColumnTransformations columnRules : allColumnRules) {
            transformedRow = columnRules.applyRules(transformedRow);
        }
        return transformedRow;
    }

    public JavaRDD<String> applyRowTransforms(JavaRDD<String> dataset) {
        JavaRDD<String> transformedDataset = dataset;
        for (RowTransformation rowTransform : allRowRules) {
            transformedDataset = rowTransform.apply(transformedDataset);
        }
        return transformedDataset;

    }

    public void deduplicateRows() {
        allRowRules.add(new DeduplicationTransformation());
    }

    public void removeRows(RowPredicate condition) {
        allRowRules.add(new RowRemovalTransformation(condition));
    }

    public void appendRows(String newFile, String destinationFile) {

    }
}
