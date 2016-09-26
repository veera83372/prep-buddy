package com.thoughtworks.datacommons.prepbuddy.clusterers

import com.thoughtworks.datacommons.prepbuddy.utils.Range
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * TextFacets is a collection of unique strings and the number of times the string appears in a column
  */
class TextFacets(facets: RDD[(String, Int)]) {
    private val tuples: Array[(String, Int)] = facets.collect()

    def getFacetsBetween(lowerBound: Int, upperBound: Int): Array[(String, Int)] = {
        tuples.filter(tuple => new Range(lowerBound, upperBound).contains(tuple._2))
    }

    def lowest: Array[(String, Int)] = getPeakListFor(_ < _)

    def highest: Array[(String, Int)] = getPeakListFor(_ > _)

    private def getPeakListFor(compareFunction: (Int, Int) => Boolean): Array[(String, Int)] = {
        var peakTuple: (String, Int) = tuples.find { case (uniqueString, count) => !uniqueString.trim.isEmpty }.head
        var facetsCount: ListBuffer[(String, Int)] = ListBuffer(peakTuple)
        tuples.view
            .filter { case (uniqueString, count) => !uniqueString.trim.isEmpty }
            .foreach((tuple) => {
                if (compareFunction(tuple._2, peakTuple._2)) {
                    peakTuple = tuple
                    facetsCount = ListBuffer(peakTuple)
                }
                if ((tuple._2 == peakTuple._2) && (tuple != peakTuple)) {
                    facetsCount += tuple
                }
            })
        facetsCount.toArray
    }

    def count: Long = facets.count

    def cardinalValues: Array[String] = tuples.map(_._1)

    def rdd: RDD[(String, Int)] = facets
}
