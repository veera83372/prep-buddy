package com.thoughtworks.datacommons.prepbuddy.analyzers.schema

import org.apache.spark.sql.types.StructField

class SchemaComplianceProfile(nonCompliantSchema: Array[(StructField, StructField)], fieldReports: Array[FieldReport]) {
    def schemaDifference: Array[(StructField, StructField)] = nonCompliantSchema
    
    def reportFor(expectedColumnName: String): FieldReport = {
        fieldReports.find(_.isColumnName(expectedColumnName)).orNull
    }
}
