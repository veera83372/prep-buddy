package org.apache.datacommons.prepbuddy.analyzers

import org.apache.datacommons.prepbuddy.types.FileType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}

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
