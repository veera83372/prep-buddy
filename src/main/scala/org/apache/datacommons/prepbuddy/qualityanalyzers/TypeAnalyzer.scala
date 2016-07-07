package org.apache.datacommons.prepbuddy.qualityanalyzers

class TypeAnalyzer(sampleData: List[String]) {

    private val PATTERN: String = "^([+-]?\\d+?\\s?)(\\d*(\\.\\d+)?)+$"

    def getType: DataType = {
        val baseType: BaseDataType = getBaseType(sampleData)
        baseType.actualType(sampleData)
    }

    private def getBaseType(samples: List[String]): BaseDataType = {
        if (matchesWith(PATTERN, samples)) return NUMERIC
        STRING
    }

    private def matchesWith(regex: String, samples: List[String]): Boolean = {
        var counter: Int = 0
        val threshold: Int = samples.length / 2
        for (string <- samples) if (string.matches(regex)) counter += 1
        counter >= threshold
    }
}
