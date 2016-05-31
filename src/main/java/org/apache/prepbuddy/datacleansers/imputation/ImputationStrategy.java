package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;

import java.io.Serializable;

public interface ImputationStrategy extends Serializable {
    void prepareSubstitute(TransformableRDD rdd, int missingDataColumn);

    String handleMissingData(RowRecord record);
}
