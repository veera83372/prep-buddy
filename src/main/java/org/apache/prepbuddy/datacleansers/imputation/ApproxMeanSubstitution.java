package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaDoubleRDD;

public class ApproxMeanSubstitution implements ImputationStrategy {
    private Double approximateMean;

    @Override
    public void prepareSubstitute(TransformableRDD rdd, int columnIndex) {
        int timeout = 20000;
        JavaDoubleRDD javaDoubleRDD = rdd.toDoubleRDD(columnIndex);
        this.approximateMean = javaDoubleRDD.meanApprox(timeout).getFinalValue().mean();
    }

    @Override
    public String handleMissingData(RowRecord record) {
        return approximateMean.toString();
    }
}
