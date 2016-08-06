package com.thoughtworks.datacommons.prepbuddy.imputations

import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord

/**
  * A contract for an imputation strategy
  */
trait ImputationStrategy extends Serializable {
    def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int)
    
    def handleMissingData(record: RowRecord): String
}
