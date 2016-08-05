package com.thoughtworks.datacommons.prepbuddy.analyzers.schema

import com.thoughtworks.datacommons.prepbuddy.analyzers.schema.datatypes.FieldType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class FieldReport(expected: StructField, actual: StructField, spark: SparkSession) extends Serializable {
    def getMissMatchedData(dataframe: DataFrame): DataFrame = {
        import spark.implicits._
        val valuesUnderInspection: Dataset[String] = dataframe.select(actual.name).map(_ (0).toString)
        val missMatchedValues: Dataset[String] = valuesUnderInspection
            .filter(FieldType.inferField(_) != expected.dataType)
        val currentColumnName: String = missMatchedValues.schema.fields.head.name
        missMatchedValues.withColumnRenamed(currentColumnName, actual.name)
    }
}
