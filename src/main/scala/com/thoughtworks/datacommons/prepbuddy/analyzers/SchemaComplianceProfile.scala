package com.thoughtworks.datacommons.prepbuddy.analyzers

import org.apache.spark.sql.types.StructField

class SchemaComplianceProfile(nonCompliantSchema: Array[(StructField, StructField)]) {
    var missmatches: Array[(StructField, StructField)] = nonCompliantSchema

}
