package com.thoughtworks.datacommons.prepbuddy.analyzers

import com.thoughtworks.datacommons.prepbuddy.analyzers.schema.FieldReport
import com.thoughtworks.datacommons.prepbuddy.analyzers.schema.datatypes.FieldType
import com.thoughtworks.datacommons.prepbuddy.analyzers.types.FileType
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StructField, StructType}

class AnalyzableDataset(spark: SparkSession, filePath: String, fileType: FileType, header: Boolean = true) {
    
    private val dataset: Dataset[Row] = fileType.read(spark, filePath, header)
    
    def analyzeColumn(columnName: String, rules: ColumnRules): ColumnProfile = {
        new ColumnProfile()
    }
    
    def analyzeCompleteness(definition: RowCompletenessRule): RowCompletenessProfile = {
        new RowCompletenessProfile()
    }
    
    def analyzeSchemaCompliance(expectedSchema: StructType): SchemaComplianceProfile = {
        val actualFields: Array[StructField] = dataset.schema.fields
        val mismatchedFields: Array[(StructField, StructField)] = expectedSchema.fields
            .zip(actualFields)
            .filter(fieldPair => fieldPair._1 != fieldPair._2)
    
        val mismatchedFieldReports: Array[FieldReport] = mismatchedFields.map {
            case (expected: StructField, actual: StructField) =>
                new FieldReport(expected, actual, unsatisfiedValuesAt(actual, expected))
        }
    
        new SchemaComplianceProfile(mismatchedFields, mismatchedFieldReports)
    }
    
    private def unsatisfiedValuesAt(actual: StructField, expected: StructField): DataFrame = {
        if (actual.dataType == expected.dataType) {
            spark.emptyDataFrame.withColumn(actual.name, lit(null: String))
        }
        else {
            import spark.implicits._
            
            val stringContent: Dataset[String] = dataset.select(actual.name).map(_ (0).toString)
            val missMatchedValues: Dataset[String] = stringContent.filter(FieldType.inferField(_) != expected.dataType)
            
            val currentColumnName: String = missMatchedValues.schema.fields.head.name
            missMatchedValues.withColumnRenamed(currentColumnName, actual.name)
        }
    }
}
