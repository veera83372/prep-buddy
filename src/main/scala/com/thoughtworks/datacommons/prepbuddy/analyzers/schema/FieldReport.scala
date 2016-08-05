package com.thoughtworks.datacommons.prepbuddy.analyzers.schema

import com.thoughtworks.datacommons.prepbuddy.analyzers.schema.datatypes.FieldType
import org.apache.spark.sql.types.{DataType, StructField}
import org.apache.spark.sql.{DataFrame, Dataset}

class FieldReport(expected: StructField, actual: StructField, columnContents: DataFrame) extends Serializable {
    val spark = columnContents.sparkSession
    
    import spark.implicits._
    
    def getMissMatchedData: DataFrame = {
        val contentsAsString: Dataset[String] = columnContents.map(_ (0).toString)
        val missMatchedValues: Dataset[String] = contentsAsString.filter(FieldType.inferField(_) != expected.dataType)
        
        val currentColumnName: String = missMatchedValues.schema.fields.head.name
        missMatchedValues.withColumnRenamed(currentColumnName, actual.name)
    }
    
    def actualFieldName: String = actual.name
    
    def expectedFieldName: String = expected.name
    
    def actualDataType: DataType = actual.dataType
    
    def expectedDataType: DataType = expected.dataType
}
