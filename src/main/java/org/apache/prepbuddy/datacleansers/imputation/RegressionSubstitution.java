package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaDoubleRDD;

import java.text.DecimalFormat;

public class RegressionSubstitution implements ImputationStrategy {

    private int _XColumnIndex;
    private double slop;
    private double intercept;

    public RegressionSubstitution(int _XColumnIndex) {
        this._XColumnIndex = _XColumnIndex;
    }

    @Override
    public void prepareSubstitute(TransformableRDD rdd, final int columnIndex) {
        TransformableRDD withoutMissingValue = rdd.removeRows(new RowPurger.Predicate() {
            @Override
            public Boolean evaluate(RowRecord record) {
                String _XColumnValue = record.valueAt(_XColumnIndex);
                String _YColumnValue = record.valueAt(columnIndex);
                return _XColumnValue.trim().isEmpty() || _YColumnValue.trim().isEmpty();
            }
        });
        long count = rdd.count() - 1;
        JavaDoubleRDD _XYRdd = withoutMissingValue.toMultipliedRdd(columnIndex, _XColumnIndex);
        JavaDoubleRDD _XXRdd = withoutMissingValue.toMultipliedRdd(_XColumnIndex, _XColumnIndex);
        JavaDoubleRDD _YDoubleRdd = withoutMissingValue.toDoubleRDD(columnIndex);
        JavaDoubleRDD _XDoubleRdd = withoutMissingValue.toDoubleRDD(_XColumnIndex);

        Double squareRddSum = _XXRdd.sum();
        Double _XYRddSum = _XYRdd.sum();
        Double _YRddSum = _YDoubleRdd.sum();
        Double _XRddSum = _XDoubleRdd.sum();

        setSlop(_XRddSum, _YRddSum, _XYRddSum, squareRddSum, count);
        setIntercept(_XRddSum, _YRddSum, count);
    }

    private void setIntercept(Double xRddSum, Double yRddSum, long count) {
        intercept = (yRddSum - (slop * xRddSum)) / count;
    }

    private void setSlop(Double xRddSum, Double yRddSum, Double xyRddSum, Double squareRddSum, long count) {
        slop = ((count * xyRddSum) - (xRddSum * yRddSum)) / ((count * squareRddSum) - xRddSum * xRddSum);
    }


    @Override
    public String handleMissingData(RowRecord record) {
        Double value = Double.parseDouble(record.valueAt(_XColumnIndex));
        Double imputeValue = intercept + slop * value;
        imputeValue = Double.parseDouble(new DecimalFormat("##.##").format(imputeValue));
        return imputeValue.toString();
    }
}
