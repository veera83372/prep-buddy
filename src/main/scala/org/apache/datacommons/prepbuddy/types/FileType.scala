package org.apache.datacommons.prepbuddy.types

abstract class FileType extends Serializable {
    def join(values: Array[String]): String

    def parse(record: String): Array[String]
}

object CSV extends FileType {
    override def join(values: Array[String]): String = values.mkString(",")

    override def parse(record: String): Array[String] = record.split(",", -1).map(columnValue => columnValue.trim)
}

object TSV extends FileType {
    override def join(values: Array[String]): String = values.mkString("\t")

    override def parse(record: String): Array[String] = record.split("\t", -1).map(columnValue => columnValue.trim)
}

