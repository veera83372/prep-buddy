package org.apache.datacommons.prepbuddy.qualityanalyzers

abstract class BaseDataType extends Serializable {
    def actualType(sampleData: List[String]): DataType

    protected def checkActualType(sampleData: List[String], subtypes: Array[DataType]): DataType = {
        for (subtype <- subtypes) if (subtype.isOfType(sampleData)) return subtype
        ALPHANUMERIC_STRING
    }
}

object BaseDataType extends Serializable {
    private val PATTERN: String = "^([+-]?\\d+?\\s?)(\\d*(\\.\\d+)?)+$"

    def getBaseType(samples: List[String]): BaseDataType = {
        if (matchesWith(PATTERN, samples)) return NUMERIC
        STRING
    }

    def matchesWith(regex: String, samples: List[String]): Boolean = {
        var counter: Int = 0
        val threshold: Int = samples.length / 2
        for (string <- samples) if (string.matches(regex)) counter += 1
        counter >= threshold
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


