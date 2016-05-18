package org.apache.prepbuddy.datacleansers;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaDoubleRDD;

public class ApproxMeanStrategy implements ImputationStrategy {
    private Double approximatedMean;
    @Override
    public void prepareSubstitute(TransformableRDD rdd, int columnIndex) {
        int timeout = 20000;
        JavaDoubleRDD javaDoubleRDD = rdd.toDoubleRDD(columnIndex);
        this.approximatedMean = javaDoubleRDD.meanApprox(timeout).getFinalValue().mean();
    }

    @Override
    public String handleMissingData(RowRecord record) {
        return approximatedMean.toString();
    }
}
