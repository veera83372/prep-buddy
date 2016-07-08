package org.apache.datacommons.prepbuddy.utils

class Range(lowerBound: Double = 0, upperBound: Double = 0) {
    def contains(aValue: Double): Boolean = {
        aValue >= lowerBound && aValue <= upperBound
    }

    def lowerToUpperValues: List[Int] = {
        var continuousValues: List[Int] = List()
        var value: Int = lowerBound.toInt
        while (value <= upperBound) {
            continuousValues = continuousValues.:+(value)
            value += 1
        }
        continuousValues
    }
}

