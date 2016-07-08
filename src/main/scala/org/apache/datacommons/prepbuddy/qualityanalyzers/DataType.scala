package org.apache.datacommons.prepbuddy.qualityanalyzers

abstract class DataType {
    def matchingCount(sampleData: List[String]): Int

    protected def matches(regex: String, samples: List[String]): Int = {
        val matches: List[String] = samples.filter(_.matches(regex))
        matches.size
    }
}

object ALPHANUMERIC_STRING extends DataType {
    override def matchingCount(sampleData: List[String]): Int = sampleData.size
}

object DECIMAL extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^[+-]?(\\.\\d+|\\d+\\.\\d+)$"
        matches(EXPRESSION, sampleData)
    }
}

object INTEGER extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^[+-]?\\d+$"
        matches(EXPRESSION, sampleData)
    }
}

object EMAIL extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^[_A-Za-z0-9-]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$"
        matches(EXPRESSION, sampleData)
    }
}

object CURRENCY extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^(\\p{Sc})(\\d+|\\d+.\\d+)$"
        matches(EXPRESSION, sampleData)
    }
}
