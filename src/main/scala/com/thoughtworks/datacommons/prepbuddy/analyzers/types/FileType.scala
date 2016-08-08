package com.thoughtworks.datacommons.prepbuddy.analyzers.types

import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class FileType {
    def read(spark: SparkSession, filePath: String, hasHeader: Boolean): Dataset[Row]
}

object CSV extends FileType {
    override def read(spark: SparkSession, filePath: String, header: Boolean): Dataset[Row] = {
        spark.read
            .format("com.databricks.spark.csv")
            .option("header", header.toString)
            .option("inferSchema", "true")
            .load(filePath)
    }
}
