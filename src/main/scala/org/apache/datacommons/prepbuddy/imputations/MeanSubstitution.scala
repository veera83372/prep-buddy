package org.apache.datacommons.prepbuddy.imputations

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.utils.RowRecord

/**
  * An imputation strategy that imputes the missing column value with the
  * mean of the specified column.
  */
class MeanSubstitution extends ImputationStrategy {
    private var mean: Double = 0

    def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int): Unit = {
        mean = rdd.toDoubleRDD(missingDataColumn).mean()
    }

    def handleMissingData(record: RowRecord): String = mean.toString
}
