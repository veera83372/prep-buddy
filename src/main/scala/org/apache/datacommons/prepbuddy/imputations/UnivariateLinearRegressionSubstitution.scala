package org.apache.datacommons.prepbuddy.imputations

import org.apache.commons.lang.math.NumberUtils
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.utils.RowRecord

class UnivariateLinearRegressionSubstitution(independentColumn: Int) extends ImputationStrategy{
    override def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int): Unit = {
        val rddForRegression: TransformableRDD = rdd.removeRows((record) => {
            val yColumnValue: String = record.valueAt(missingDataColumn)
            val xColumnValue: String = record.valueAt(independentColumn)
            !NumberUtils.isNumber(xColumnValue) || xColumnValue.trim.isEmpty || yColumnValue.trim.isEmpty
        })
//        rddForRegression.
    }

    override def handleMissingData(record: RowRecord): String = ???
}
