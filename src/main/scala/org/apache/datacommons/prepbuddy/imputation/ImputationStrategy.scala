package org.apache.datacommons.prepbuddy.imputation

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.utils.RowRecord

trait ImputationStrategy extends Serializable{
  def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int)

  def handleMissingData(record: RowRecord): String
}
