package org.apache.datacommons.prepbuddy.clusterers

import org.apache.spark.rdd.RDD

class TextFacets(facets: RDD[(String, Int)]) {
    private val tuples: Array[(String, Int)] = facets.collect()

    def getFacetsBetween(lowerBound: Int, upperBound: Int): Array[(String, Int)] = {
        val tuplesBetween: Array[(String, Int)] = tuples.filter((tuple) => {
            val currentTuple = tuple._2
            isInRange(currentTuple, lowerBound, upperBound)
        })
        tuplesBetween
    }

    def lowest: Array[(String, Int)] = {
        getPeakListFor((currentTuple, peakTuple) => {
            currentTuple < peakTuple
        })
    }

    def highest: Array[(String, Int)] = {
        getPeakListFor((currentTuple, peakTuple) => {
            currentTuple > peakTuple
        })
    }

    def getPeakListFor(compareFunction: (Int, Int) => Boolean): Array[(String, Int)] = {
        var list: Array[(String, Int)] = Array()
        var peakTuple: (String, Int) = tuples(0)
        list = list.:+(peakTuple)
        tuples.foreach((tuple)=>{
            if (compareFunction(tuple._2, peakTuple._2)) {
                peakTuple = tuple
                list = list.drop(list.length)
                list = list.:+(peakTuple)
            }
            if ((tuple._2 == peakTuple._2) && !(tuple == peakTuple)) {
                list = list.:+(tuple)
            }
        })
        list
    }

    private def isInRange(currentTupleValue: Integer, minimum: Int, maximum: Int): Boolean = {
        currentTupleValue >= minimum && currentTupleValue <= maximum
    }

    def count: Long = facets.count
}
