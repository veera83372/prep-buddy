package com.thoughtworks.datacommons.prepbuddy.analyzers

import com.thoughtworks.datacommons.prepbuddy.analyzers.schema.FieldReport
import com.thoughtworks.datacommons.prepbuddy.analyzers.schema.datatypes.FieldType
import com.thoughtworks.datacommons.prepbuddy.exceptions.{ApplicationException, ErrorMessages}
import com.thoughtworks.datacommons.prepbuddy.types.FileType
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StructField, StructType}

class AnalyzableDataset(spark: SparkSession, filePath: String, fileType: FileType, header: Boolean = true) {
    
    private val dataset: Dataset[Row] = fileType.read(spark, filePath, header).cache
    
    def analyzeColumn(columnName: String, rules: ColumnRules): ColumnProfile = {
        new ColumnProfile()
    }
    
    private val first: Row = dataset.first()
    
    def analyzeCompleteness(definition: RowCompletenessRule): RowCompletenessProfile = {
        val totalNumberOfRows: Long = dataset.count
        val numberOfIncomplete: Long = dataset.filter(!definition.isComplete(_)).count
        new RowCompletenessProfile(numberOfIncomplete, totalNumberOfRows)
    }
    
    def analyzeSchemaCompliance(expectedSchema: StructType): SchemaComplianceProfile = {
        val actualFields: Array[StructField] = dataset.schema.fields
        
        if (actualFields.length != expectedSchema.fields.length) {
            throw new ApplicationException(ErrorMessages.NUMBER_OF_COLUMN_DID_NOT_MATCHED)
        }
        
        val mismatchedColumnMetadata: Array[(StructField, StructField)] = expectedSchema.fields
            .zip(actualFields)
            .filter { case (expected, actual) => expected != actual }
        
        val mismatchedColumnData: Array[FieldReport] = mismatchedColumnMetadata.map {
            case (expected: StructField, actual: StructField) =>
                new FieldReport(expected, actual, getMismatchedRecords(actual, expected))
        }
        
        new SchemaComplianceProfile(mismatchedColumnMetadata, mismatchedColumnData)
    }
    
    private def getMismatchedRecords(actual: StructField, expected: StructField): DataFrame = {
        if (actual.dataType == expected.dataType) {
            spark.emptyDataFrame.withColumn(actual.name, lit(null: String))
        } else {
            dataset
                .select(actual.name)
                .filter(record => {
                    val stringValue: String = if (record.anyNull) null else record(0).toString
                    FieldType.inferField(stringValue) != expected.dataType
                }).toDF
        }
    }
}
