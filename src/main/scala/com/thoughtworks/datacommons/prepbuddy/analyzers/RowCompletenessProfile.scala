package com.thoughtworks.datacommons.prepbuddy.analyzers

class RowCompletenessProfile(numberOfCompleteRow: Long, totalNumberOfRow: Long) {
    def percentageOfCompleteness: Double = {
        val result: Double = (numberOfCompleteRow.toDouble / totalNumberOfRow.toDouble) * 100
        (result * 10000).round / 10000.toDouble
    }
}
