package org.apache.datacommons.prepbuddy.types

class FileType(delimiter: String) extends Serializable {
    def appendDelimiter(row: String): String = row + delimiter

    def join(values: Array[String]): String = values.mkString(delimiter)

    def parse(record: String): Array[String] = record.split(delimiter, -1).map(_.trim)

    def valueAt(record: String, index: Int): String = parse(record)(index)
}

object CSV extends FileType(",")

object TSV extends FileType("\t")

