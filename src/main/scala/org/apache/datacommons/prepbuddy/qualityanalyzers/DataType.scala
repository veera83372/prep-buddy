package org.apache.datacommons.prepbuddy.qualityanalyzers

abstract class DataType {
    def isOfType(sampleData: List[String]): Boolean

    def matchesCriteria(regex: String, samples: List[String]): Boolean = {
        val matches: List[String] = samples.filter(_.matches(regex))
        val threshold = Math.round(samples.length * 0.75)
        //DEBUG println(matches.size)
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

object INTEGER extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^[+-]?\\d+$"
        matchesCriteria(EXPRESSION, sampleData)
    }
}

object EMAIL extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^[_A-Za-z0-9-]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$"
        matchesCriteria(EXPRESSION, sampleData)
    }
}
