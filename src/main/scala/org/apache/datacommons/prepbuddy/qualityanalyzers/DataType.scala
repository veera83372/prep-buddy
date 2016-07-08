package org.apache.datacommons.prepbuddy.qualityanalyzers

abstract class DataType {
    def isOfType(sampleData: List[String]): Boolean

    def matchesCriteria(regex: String, samples: List[String]): Boolean = {
        val matches: List[String] = samples.filter(_.matches(regex))
        val threshold = Math.round(samples.length * 0.75)
        matches.size > threshold
    }
}

object ALPHANUMERIC_STRING extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = true
}

object DECIMAL extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^[+-]?(\\.\\d+|\\d+\\.\\d+)$"
        matchesCriteria(EXPRESSION, sampleData)
    }
}
