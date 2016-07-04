package org.apache.datacommons.prepbuddy.clusterers

import org.apache.spark.rdd.RDD

class TextFacets(facets: RDD[(String, Int)]) {
    private val tuples: Array[(String, Int)] = facets.collect()

    def getFacetsBetween(lowerBound: Int, upperBound: Int): Array[(String, Int)] = {
        tuples.filter((tuple) => {
            val currentTupleCount = tuple._2
            isInRange(currentTupleCount, lowerBound, upperBound)
        })
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

    private def getPeakListFor(compareFunction: (Int, Int) => Boolean): Array[(String, Int)] = {
        var facetsCount: Array[(String, Int)] = Array()
        var peakTuple: (String, Int) = tuples(0)
        facetsCount = facetsCount.:+(peakTuple)
        tuples.foreach((tuple)=>{
            if (compareFunction(tuple._2, peakTuple._2)) {
                peakTuple = tuple
                facetsCount = facetsCount.drop(facetsCount.length)
                facetsCount = facetsCount.:+(peakTuple)
            }
            if ((tuple._2 == peakTuple._2) && !(tuple == peakTuple)) {
                facetsCount = facetsCount.:+(tuple)
            }
        })
        facetsCount
    }

    private def isInRange(currentTupleValue: Integer, minimum: Int, maximum: Int): Boolean = {
        currentTupleValue >= minimum && currentTupleValue <= maximum
    }

    def count: Long = facets.count

    def cardinalValues: Array[String] = {
        var cardinalValues: Array[String] = Array()
        tuples.foreach((tuple)=> cardinalValues = cardinalValues.:+(tuple._1))
        cardinalValues
    }
    def rdd: RDD[(String, Int)] = facets
}
