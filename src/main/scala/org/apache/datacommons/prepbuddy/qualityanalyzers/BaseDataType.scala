package org.apache.datacommons.prepbuddy.qualityanalyzers

abstract class BaseDataType extends Serializable {
    def actualType(sampleData: List[String]): DataType

    protected def checkActualType(sampleData: List[String], subtypes: Array[DataType]): DataType = {
        val tuples: Array[(DataType, Int)] = subtypes.map(dataType => (dataType, dataType.matchingCount(sampleData)))
        val reduce: (DataType, Int) = tuples.reduce((tuple, another) => {
            if (tuple._2 > another._2) tuple else another
        })
        reduce._1
    }
}

object STRING extends BaseDataType {
    val subtypes: Array[DataType] = Array(EMAIL, ALPHANUMERIC_STRING)

    override def actualType(sampleData: List[String]): DataType = {
        super.checkActualType(sampleData, subtypes)
    }
}

object NUMERIC extends BaseDataType {
    val subtypes: Array[DataType] = Array(INTEGER, DECIMAL)

    override def actualType(sampleData: List[String]): DataType = {
        super.checkActualType(sampleData, subtypes)
    }
}


