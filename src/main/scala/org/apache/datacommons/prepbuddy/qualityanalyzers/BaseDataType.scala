package org.apache.datacommons.prepbuddy.qualityanalyzers

abstract class BaseDataType extends Serializable {
    def actualType(sampleData: List[String]): DataType

    protected def checkActualType(sampleData: List[String], subtypes: Array[DataType]): DataType = {
        subtypes.find(_.isOfType(sampleData)).getOrElse(ALPHANUMERIC_STRING)
    }
}

object STRING extends BaseDataType {
    val subtypes: Array[DataType] = Array(
        EMPTY,
        CURRENCY,
        EMAIL,
        URL,
        SOCIAL_SECURITY_NUMBER,
        ZIP_CODE_US,
        COUNTRY_CODE_2_CHARACTER,
        COUNTRY_CODE_3_CHARACTER,
        COUNTRY_NAME,
        TIMESTAMP
    )

    override def actualType(sampleData: List[String]): DataType = {
        super.checkActualType(sampleData, subtypes)
    }
}

object NUMERIC extends BaseDataType {
    val subtypes: Array[DataType] = Array(
        ZIP_CODE_US,
        MOBILE_NUMBER,
        INTEGER,
        IP_ADDRESS,
        LATITUDE,
        LONGITUDE,
        DECIMAL
    )

    override def actualType(sampleData: List[String]): DataType = {
        super.checkActualType(sampleData, subtypes)
    }
}

object CATEGORICAL extends BaseDataType {
    val subtypes: Array[DataType] = Array(CATEGORICAL_INTEGER, CATEGORICAL_STRING, EMPTY)

    override def actualType(sampleData: List[String]): DataType = {
        super.checkActualType(sampleData, subtypes)
    }
}


