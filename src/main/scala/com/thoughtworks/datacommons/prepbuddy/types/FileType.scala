package com.thoughtworks.datacommons.prepbuddy.types

import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * File formats that are supported by TransformableRDD
  */
class FileType(delimiter: String) extends Serializable {
    def appendDelimiter(row: String): String = row + delimiter
    
    def join(record: RowRecord): String = record.mkString(delimiter)
    
    def parse(record: String): RowRecord = new RowRecord(record.split(delimiter, -1).map(_.trim))
    
    def read(spark: SparkSession, filePath: String, hasHeader: Boolean): Dataset[Row] = {
        spark.read
            .option("header", hasHeader)
            .option("delimiter", delimiter)
            .option("inferSchema", true)
            .csv(filePath)
    }
}

/**
  * Used to parse the record with comma(,) separated values
  */
object CSV extends FileType(",")

/**
  * Used to parse the record with tab separated values
  */
object TSV extends FileType("\t")

/**
  * Can be used to set custom delimiter
  *
  * @param delimiter A string or regex that will be used to parse each record
  */
case class CustomFileType(delimiter: String) extends FileType(delimiter)