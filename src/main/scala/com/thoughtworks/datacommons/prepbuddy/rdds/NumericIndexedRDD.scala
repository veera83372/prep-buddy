package com.thoughtworks.datacommons.prepbuddy.rdds

import org.apache.spark.rdd.RDD

class NumericIndexedRDD(doubleRDD: RDD[Double]) {
    private val sortedRDDWithIndex = doubleRDD.sortBy(x => x, ascending = true).zipWithIndex()
    private val RDDWithIndex = sortedRDDWithIndex.map { case (rowValue, rowIndex) => (rowIndex, rowValue) }.cache
    private val totalCount = RDDWithIndex.count
    private val secondQuartileIndex = getMedianIndex(totalCount)
    private val firstQuartileIndex = getMedianIndex(secondQuartileIndex - 1)
    private val thirdQuartileIndex = firstQuartileIndex + secondQuartileIndex

    def lookup(key: Long): Double = RDDWithIndex.lookup(key).head

    def interQuartileRange: Double = thirdQuartileValue - firstQuartileValue

    def firstQuartileValue: Double = lookup(firstQuartileIndex)

    def thirdQuartileValue: Double = lookup(thirdQuartileIndex)

    private def getMedianIndex(count: Long): Long = {
        def isOdd(num: Long): Boolean = num % 2 != 0
        val middleIndex = count / 2
        if (isOdd(count)) return middleIndex
        middleIndex + 1
    }

}

