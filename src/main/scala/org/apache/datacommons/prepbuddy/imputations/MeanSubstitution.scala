package org.apache.datacommons.prepbuddy.imputations

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.utils.RowRecord

class MeanSubstitution extends strategy {
    private var mean: Double = 0

    def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int): Unit = {
        mean = rdd.toDoubleRDD(missingDataColumn).mean()
    }

    def handleMissingData(record: RowRecord): String = mean.toString
}
