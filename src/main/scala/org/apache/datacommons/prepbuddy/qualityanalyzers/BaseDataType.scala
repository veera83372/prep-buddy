package org.apache.datacommons.prepbuddy.qualityanalyzers

abstract class BaseDataType extends Serializable {
    def actualType(sampleData: List[String]): DataType

    protected def checkActualType(sampleData: List[String], subtypes: Array[DataType]): DataType = {
        subtypes.find(_.isOfType(sampleData)).getOrElse(ALPHANUMERIC_STRING)
    }
}

object STRING extends BaseDataType {
    val subtypes: Array[DataType] = Array(ALPHANUMERIC_STRING)

    override def actualType(sampleData: List[String]): DataType = {
        super.checkActualType(sampleData, subtypes)
    }
}

object NUMERIC extends BaseDataType {
    val subtypes: Array[DataType] = Array(DECIMAL)

    override def actualType(sampleData: List[String]): DataType = {
        super.checkActualType(sampleData, subtypes)
    }
}


