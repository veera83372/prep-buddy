package org.apache.datacommons.prepbuddy.analyzers

import org.apache.spark.sql.Dataset

class SchemaComplianceProfile {

    private var nonCompliantRecords: Dataset = null
    private var nonCompliancePercentage: Int = 0

}
