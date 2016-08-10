package com.thoughtworks.datacommons.prepbuddy.analyzers

class RowCompletenessProfile(numberOfIncompleteRow: Long, totalNumberOfRow: Long) {
    def percentage: Double = {
        val result: Double = (numberOfIncompleteRow.toDouble / totalNumberOfRow.toDouble) * 100
        (result * 10000).round / 10000.toDouble
    }
    
    private var completenessPercentage: Int = 0
}
