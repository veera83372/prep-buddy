package org.apache.datacommons.prepbuddy.qualityanalyzers

class TypeAnalyzer(sampleData: List[String]) {
    def getType: DataType = {
        val baseType: BaseDataType = BaseDataType.getBaseType(sampleData)
        baseType.actualType(sampleData)
    }
}
