package com.thoughtworks.datacommons.prepbuddy.types

import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord

import scala.collection.mutable.ListBuffer

/**
  * File formats that are supported by TransformableRDD
  */
class FileType(delimiter: String) extends Serializable {

    val ESCAPE_SEQUENCE: String = "\""

    def appendDelimiter(row: String): String = row + delimiter

    def join(record: RowRecord): String = record.mkString(delimiter) //todo: Need to check delimiter before Joining

    def parse(record: String): RowRecord = {
        var parsedRecord: ListBuffer[String] = new ListBuffer[String]()
        var indexOfDelimiter = 0
        var escapingCharLength = 1
        var tempString = record + delimiter
        while (tempString.length > 0) {
            if (tempString.startsWith(ESCAPE_SEQUENCE)) {
                tempString = tempString.tail
                indexOfDelimiter = tempString.indexOf(ESCAPE_SEQUENCE)
                escapingCharLength = 2
            }
            else {
                indexOfDelimiter = tempString.indexOf(delimiter)
                escapingCharLength = 1
            }
            parsedRecord += tempString.take(indexOfDelimiter).trim
            tempString = tempString.substring(indexOfDelimiter + escapingCharLength, tempString.length)
        }
        new RowRecord(parsedRecord.toArray)
    }
}

object CSV extends FileType(",")

object TSV extends FileType("\t")

