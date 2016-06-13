package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaDoubleRDD;

import java.text.DecimalFormat;

public class UnivariateLinearRegressionSubstitution implements ImputationStrategy {

    public static final String BLANK_STRING = "";
    private int independentColumnIndex;
    private double slope;
    private double intercept;

    public UnivariateLinearRegressionSubstitution(int _XColumnIndex) {
        this.independentColumnIndex = _XColumnIndex;
    }

    @Override
    public void prepareSubstitute(TransformableRDD inputDataset, final int missingDataColumnIndex) {
        TransformableRDD rddForRegression = inputDataset.removeRows(new RowPurger.Predicate() {
            @Override
            public Boolean evaluate(RowRecord record) {
                String _XColumnValue = record.valueAt(independentColumnIndex);
                String _YColumnValue = record.valueAt(missingDataColumnIndex);
                return _XColumnValue.trim().isEmpty() || _YColumnValue.trim().isEmpty();
            }
        });
        long count = rddForRegression.count();

        JavaDoubleRDD xyRDD = rddForRegression.multiplyColumns(missingDataColumnIndex, independentColumnIndex);
        JavaDoubleRDD xSquaredRDD = rddForRegression.multiplyColumns(independentColumnIndex, independentColumnIndex);
        JavaDoubleRDD yRDD = rddForRegression.toDoubleRDD(missingDataColumnIndex);
        JavaDoubleRDD xRDD = rddForRegression.toDoubleRDD(independentColumnIndex);

        Double squareRddSum = xSquaredRDD.sum();
        Double sumOfXY = xyRDD.sum();
        Double sumOfY = yRDD.sum();
        Double sumOfX = xRDD.sum();

        setSlope(sumOfX, sumOfY, sumOfXY, squareRddSum, count);
        setIntercept(sumOfX, sumOfY, count);
    }

    private void setIntercept(Double sumOfXs, Double sumOfYs, long count) {
        intercept = (sumOfYs - (slope * sumOfXs)) / count;
    }

    private void setSlope(Double sumOfXs, Double sumOfYs, Double sumOfXYs, Double sumOfXSquared, long count) {
        slope = ((count * sumOfXYs) - (sumOfXs * sumOfYs)) / ((count * sumOfXSquared) - sumOfXs * sumOfXs);
    }

    @Override
    public String handleMissingData(RowRecord record) {
        String dependentValue = record.valueAt(independentColumnIndex);
        try {
            Double value = Double.parseDouble(dependentValue);
            Double imputedValue = intercept + slope * value;
            imputedValue = Double.parseDouble(new DecimalFormat("##.##").format(imputedValue));
            return imputedValue.toString();
        } catch (NumberFormatException e) {
            return BLANK_STRING;
        }
    }
}
