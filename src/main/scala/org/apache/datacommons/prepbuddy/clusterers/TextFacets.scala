package org.apache.datacommons.prepbuddy.clusterers

import org.apache.spark.rdd.RDD

class TextFacets(facets: RDD[(String, Int)]) {
    private val tuples: Array[(String, Int)] = facets.collect()

    def getFacetsBetween(lowerBound: Int, upperBound: Int): Array[(String, Int)] = {
        tuples.filter((tuple) => isInRange(tuple._2, lowerBound, upperBound))
    }

    private def isInRange(currentTupleValue: Integer, minimum: Int, maximum: Int): Boolean = {
        currentTupleValue >= minimum && currentTupleValue <= maximum
    }

    def lowest: Array[(String, Int)] = {
        getPeakListFor((currentTuple, peakTuple) => currentTuple < peakTuple)
    }

    def highest: Array[(String, Int)] = {
        getPeakListFor((currentTuple, peakTuple) => currentTuple > peakTuple)
    }

    private def getPeakListFor(compareFunction: (Int, Int) => Boolean): Array[(String, Int)] = {
        val option: Option[(String, Int)] = tuples.find(!_._1.isEmpty)
        var peakTuple = option.head
        var facetsCount: Array[(String, Int)] = Array(peakTuple)
        tuples.foreach((tuple) => {
            if (compareFunction(tuple._2, peakTuple._2) && !tuple._1.trim.isEmpty) {
                peakTuple = tuple
                facetsCount = Array(peakTuple)
            }
            if ((tuple._2 == peakTuple._2) && !(tuple == peakTuple) && !tuple._1.trim.isEmpty) {
                facetsCount = facetsCount.:+(tuple)
            }
        })
        facetsCount
    }

    def count: Long = facets.count

    def cardinalValues: Array[String] = tuples.map(_._1)

    def rdd: RDD[(String, Int)] = facets
}
