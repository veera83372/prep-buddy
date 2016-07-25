package org.apache.datacommons.prepbuddy.imputations

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.utils.RowRecord

/**
  * An imputation strategy that imputes the missing column value by
  * approx mean of the specified column.
  */
class ApproxMeanSubstitution() extends ImputationStrategy {
    private var approxMean: Double = 0

    override def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int): Unit = {
        val timeOut: Int = 20000
        approxMean = rdd.toDoubleRDD(missingDataColumn).meanApprox(timeOut).getFinalValue().mean
    }

    override def handleMissingData(record: RowRecord): String = approxMean.toString
}
