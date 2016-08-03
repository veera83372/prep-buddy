package org.datacommons.prepbuddy.analyzers

import org.apache.spark.sql.Dataset

class SchemaComplianceProfile {

    private var nonCompliantRecords: Dataset[String] = null
    private var nonCompliancePercentage: Int = 0

}
