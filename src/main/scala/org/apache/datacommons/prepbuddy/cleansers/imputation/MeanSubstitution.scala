package org.apache.datacommons.prepbuddy.cleansers.imputation

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.utils.RowRecord

class MeanSubstitution extends ImputationStrategy {
    private var mean: Double = 0

    def handleMissingData(record: RowRecord): String = mean.toString

    def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int): Unit = {
        mean = rdd.toDoubleRDD(missingDataColumn).mean()
    }
}
