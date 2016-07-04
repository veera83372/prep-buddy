package org.apache.datacommons.prepbuddy.imputations

import org.apache.datacommons.prepbuddy.clusterers.TextFacets
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.utils.RowRecord

class NaiveBayesSubstitution(independentColumnIndexes: Int*) extends ImputationStrategy{
    override def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int): Unit = {
        val trainingSet: TransformableRDD = rdd.removeRows((record) => {
            record.hasEmptyColumn
        })
        val facets: TextFacets = trainingSet.listFacets(missingDataColumn)
        val permissibleValues: Array[String] = facets.cardinalValues

    }

    override def handleMissingData(record: RowRecord): String = ???
}
