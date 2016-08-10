package com.thoughtworks.datacommons.prepbuddy.analyzers

class RowCompletenessProfile(totalNumberOfRow: Int, numberOfInCompleteRow: Int) {
    def percentage: Double = (numberOfInCompleteRow / totalNumberOfRow) * 100
    
    private var completenessPercentage: Int = 0
}
