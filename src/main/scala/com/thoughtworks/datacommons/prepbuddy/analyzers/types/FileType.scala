package com.thoughtworks.datacommons.prepbuddy.analyzers.types

import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class FileType {
    def read(spark: SparkSession, filePath: String, hasHeader: Boolean): Dataset[Row]
}

object CSV extends FileType {
    override def read(spark: SparkSession, filePath: String, header: Boolean): Dataset[Row] = {
        spark.read
            .option("header", header.toString)
            .option("inferSchema", "true")
            .csv(filePath)
    }
}
