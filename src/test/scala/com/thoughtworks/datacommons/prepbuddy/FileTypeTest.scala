package com.thoughtworks.datacommons.prepbuddy

import com.thoughtworks.datacommons.prepbuddy.types.{CSV, CustomFileType, TSV}
import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord

class FileTypeTest extends SparkTestCase {

    test("should be able to parse csv record") {
        val inputRecord: String = "Ram,Sita,Lakshman"

        val parsedRecord: RowRecord = CSV.parse(inputRecord)

        assertResult(3)(parsedRecord.length)
        assertResult("Ram")(parsedRecord(0))
        assertResult("Sita")(parsedRecord(1))
        assertResult("Lakshman")(parsedRecord(2))
    }

    test("should be able to parse tsv record") {
        val inputRecord: String = "Ram\tSita\tLakshman"

        val parsedRecord: RowRecord = TSV.parse(inputRecord)

        assertResult(3)(parsedRecord.length)
        assertResult("Ram")(parsedRecord(0))
        assertResult("Sita")(parsedRecord(1))
        assertResult("Lakshman")(parsedRecord(2))
    }

    test("should be able to parse any delimiter separated record") {
        val inputRecord: String = "Ram-Sita-Lakshman"

        val parsedRecord: RowRecord = CustomFileType("-").parse(inputRecord)

        assertResult(3)(parsedRecord.length)
        assertResult("Ram")(parsedRecord(0))
        assertResult("Sita")(parsedRecord(1))
        assertResult("Lakshman")(parsedRecord(2))
    }
}
