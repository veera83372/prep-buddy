package org.apache.datacommons.prepbuddy.qualityanalyzers

abstract class DataType {
    def isOfType(sampleData: List[String]): Boolean

    def matchesWith(regex: String, samples: List[String]): Boolean = {
        var counter: Int = 0
        val threshold: Int = samples.size / 2
        for (string <- samples) {
            if (string.matches(regex)) counter += 1
        }
        counter >= threshold
    }
}

object ALPHANUMERIC_STRING extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = true
}

object DECIMAL extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^[+-]?(\\.\\d+|\\d+\\.\\d+)$"
        matchesWith(EXPRESSION, sampleData)
    }
}
