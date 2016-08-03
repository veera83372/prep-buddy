package org.datacommons.prepbuddy.analyzers

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.datacommons.prepbuddy.types.FileType

class AnalyzableDataset(spark: SparkSession, fileName: String, fileType: FileType) {
    import spark.implicits._
    private val dataset: Dataset[String] = spark.read.text(fileName).as[String]


    def analyzeColumn(columnName: String, rules: ColumnRules): ColumnProfile = {
        new ColumnProfile()
    }

    def analyzeCompleteness(definition: RowCompletenessRule): RowCompletenessProfile = {
        new RowCompletenessProfile()
    }

    def analyzeSchemaCompliance(struct: StructType): SchemaComplianceProfile = {

        new SchemaComplianceProfile()
    }


}
