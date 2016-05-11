package org.apache.prepbuddy.coreops;

import org.apache.prepbuddy.datacleansers.Deduplication;
import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DatasetTransformations implements Serializable {
    List<ColumnTransformation> allColumnRules = new ArrayList<ColumnTransformation>();
    List<RowTransformation> allRowRules = new ArrayList<>();

    public void addColumnTransformations(ColumnTransformation columnRules) {
        allColumnRules.add(columnRules);
    }


    public String[] applyColumnTransforms(String[] untransformedRow) {
        String[] transformedRow = untransformedRow;

        for (ColumnTransformation columnRules : allColumnRules) {
            transformedRow = columnRules.apply(transformedRow);
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

    public DatasetTransformations deduplicateRows() {
        allRowRules.add(new Deduplication());
        return this;
    }

    public void removeRows(RowPurger.Predicate condition) {
        allRowRules.add(new RowPurger(condition));
    }

    public void appendRows(String newFile, String destinationFile) {

    }
}
