package com.thoughtworks.datacommons.prepbuddy.types

import com.opencsv.CSVParser
import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord

/**
  * File formats that are supported by TransformableRDD
  */
class FileType(delimiter: String) extends Serializable {
    private val escapeChar = "\""

    def appendDelimiter(row: String): String = row + delimiter

    def join(record: RowRecord): String = {
        record.map((x)=>{
            if(x.contains(delimiter)) escapeChar + x + escapeChar else x
        }).mkString(delimiter)
    }

    def parse(record: String): RowRecord = {
        val csvParser: CSVParser = new CSVParser(delimiter.charAt(0))
        val strings: Array[String] = csvParser.parseLine(record)
        new RowRecord(strings.map(_.trim))
    }
}

object CSV extends FileType(",")

object TSV extends FileType("\t")

