package com.thoughtworks.datacommons.prepbuddy.analyzers.schema

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, _}
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
            callData.select("other")
        )
        val missMatches: DataFrame = schemaReport.getMissMatchedData
        
        assert(1807 == missMatches.count())
        
        spark.stop()
    }
    
    test("should be able to find the actual column name from the miss matched data") {
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
            callData.select("other")
        )
        val missMatches: DataFrame = schemaReport.getMissMatchedData
        
        assert(callData.schema.fields(1).name == missMatches.schema.fields.head.name)
        
        spark.stop()
    }
}
