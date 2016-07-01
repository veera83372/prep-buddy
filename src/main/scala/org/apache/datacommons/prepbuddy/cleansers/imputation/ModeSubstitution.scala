package org.apache.datacommons.prepbuddy.cleansers.imputation

import org.apache.datacommons.prepbuddy.cluster.TextFacets
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.utils.RowRecord

case class ModeSubstitution() extends ImputationStrategy{
    private val mode: String = null
    override def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int): Unit = {
        val listFacets: TextFacets = rdd.listFacets(missingDataColumn)
    }

    override def handleMissingData(record: RowRecord): String = ???
}
