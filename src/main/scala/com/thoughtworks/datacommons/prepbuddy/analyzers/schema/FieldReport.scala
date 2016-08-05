package com.thoughtworks.datacommons.prepbuddy.analyzers.schema

import com.thoughtworks.datacommons.prepbuddy.analyzers.schema.datatypes.FieldType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class FieldReport(expected: StructField, actual: StructField, spark: SparkSession) extends Serializable {
    def getMissMatchedData(dataset: DataFrame): Dataset[String] = {
        import spark.implicits._
        val valuesUnderInspection: Dataset[String] = dataset.select(actual.name).map(_ (0).toString)
        valuesUnderInspection.filter(FieldType.inferField(_) != expected.dataType)
    }
}
