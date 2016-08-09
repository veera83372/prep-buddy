package com.thoughtworks.datacommons.prepbuddy.analyzers.schema

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructField}

class FieldReport(expected: StructField, actual: StructField, nonCompliantDataset: DataFrame) extends Serializable {
    def nonCompliantValues: DataFrame = nonCompliantDataset
    
    def actualFieldName: String = actual.name
    
    def expectedFieldName: String = expected.name
    
    def isColumnName(expectedColumnName: String): Boolean = expectedColumnName == expectedFieldName
    
    def actualDataType: DataType = actual.dataType
    
    def expectedDataType: DataType = expected.dataType
}
