package org.apache.datacommons.prepbuddy.qualityanalyzers

class TypeAnalyzer(sampleData: List[String]) {

    private val PATTERN: String = "^([+-.]?\\d+?\\s?)(\\d*(\\.\\d+)?)+$"

    def getType: DataType = {
        val baseType: BaseDataType = getBaseType
        baseType.actualType(sampleData)
    }

    def getBaseType: BaseDataType = {
        if (matchesNumericCriteria) return NUMERIC
        STRING
    }

    private def matchesNumericCriteria: Boolean = {
        val matches: List[String] = sampleData.filter(_.matches(PATTERN))
        val threshold = Math.round(sampleData.length * 0.75)
        matches.size >= threshold
    }
}
