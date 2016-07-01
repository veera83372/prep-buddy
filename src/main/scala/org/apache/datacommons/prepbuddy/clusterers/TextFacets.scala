package org.apache.datacommons.prepbuddy.clusterers

import org.apache.spark.rdd.RDD

class TextFacets(facets: RDD[(String, Int)]) {
    def highest: List[(String, Int)] = {
        val allTuples: Array[(String, Int)] = facets.collect()
        var list: List[(String, Int)] = List()
        var peakTuple: (String, Int) = allTuples.apply(0)
        list = list.:+(peakTuple)
        for (tuple <- allTuples) {
            if (tuple._2 > peakTuple._2) {
                peakTuple = tuple
                list = list.drop(list.size)
                list = list.:+(peakTuple)
            }
            if ((tuple._2 == peakTuple._2) && !(tuple == peakTuple)) {
                list = list.:+(tuple)
            }
        }
        list
    }

    def count: Long = facets.count
}
