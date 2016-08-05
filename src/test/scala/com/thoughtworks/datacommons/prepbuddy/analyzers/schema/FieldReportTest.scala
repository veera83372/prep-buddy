package com.thoughtworks.datacommons.prepbuddy.analyzers.schema

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession, _}
import org.scalatest.FunSuite

class FieldReportTest extends FunSuite {
    test("should return column values that did not matched the expectation") {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName(getClass.getCanonicalName)
            .getOrCreate()
        
        val callData: DataFrame = spark.read
            .format("com.databricks.spark.csv")
            .option("header", true)
            .option("inferSchema", "true")
            .load("data/calls_with_header.csv")
    
        val schemaReport: FieldReport = new FieldReport(
            StructField("Other", LongType),
            callData.schema.fields(1),
            spark
        )
        val missMatches: Dataset[String] = schemaReport.getMissMatchedData(callData)
        
        assert(1807 == missMatches.count())
        
        spark.stop()
    }
}
