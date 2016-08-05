package com.thoughtworks.datacommons.prepbuddy.analyzers

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

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
        
        new SchemaComplianceProfile(mismatchedFields)
    }
}
