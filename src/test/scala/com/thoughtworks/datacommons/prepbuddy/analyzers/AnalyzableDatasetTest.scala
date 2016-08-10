package com.thoughtworks.datacommons.prepbuddy.analyzers

import com.thoughtworks.datacommons.prepbuddy.analyzers.schema.FieldReport
import com.thoughtworks.datacommons.prepbuddy.exceptions.ApplicationException
import com.thoughtworks.datacommons.prepbuddy.types.CSV
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

//07610039694,07434677419,Incoming,211,Wed Sep 15 19:17:44 +0100 2010
//case class CallDataRecord(caller: String, callee: String, callType: String,
//                          callDuration: Long, callInitiatedAt: String)

class AnalyzableDatasetTest extends FunSuite {
    
    def sparkSession: SparkSession = {
        SparkSession
            .builder()
            .master("local[2]")
            .appName(getClass.getCanonicalName)
            .getOrCreate()
    }
    
    object CallRecord {
        private val user = StructField("user", LongType)
        private val other = StructField("Other", IntegerType)
        private val direction = StructField("direction", StringType)
        private val duration = StructField("duration", IntegerType)
        private val timestamp = StructField("timestamp", StringType)
        
        def getSchema: StructType = StructType(Array(user, other, direction, duration, timestamp))
    }
    
    ignore("shouldValidateSchema") {
        val spark = sparkSession
        
        import spark.implicits._
        val dataset: Dataset[String] = sparkSession.read.text("data/calls.csv").as[String]
        
        val analyzableDataset: AnalyzableDataset = new AnalyzableDataset(sparkSession, "data/calls.csv", CSV)
        val struct =
            StructType(
                StructField("a", IntegerType, nullable = true) ::
                    StructField("b", LongType, nullable = false) ::
                    StructField("c", BooleanType, nullable = false) :: Nil)
        
        val schemaComplianceProfile = analyzableDataset.analyzeSchemaCompliance(struct)
    
        val completenessRule: RowCompletenessRule = new RowCompletenessRule()
        val completenessProfile = analyzableDataset.analyzeCompleteness(completenessRule)
        
        val columnProfile = analyzableDataset.analyzeColumn("column-name", new ColumnRules())
    }
    
    
    test("SCHEMA: should be able to find the difference of schema") {
        val spark = sparkSession
        
        val callRecord: AnalyzableDataset = new AnalyzableDataset(spark, "data/calls_with_header.csv", CSV)
        val schemaComplianceProfile: SchemaComplianceProfile = callRecord.analyzeSchemaCompliance(CallRecord.getSchema)
        
        val expected: Array[(StructField, StructField)] = Array(
            (StructField("Other", IntegerType), StructField("other", LongType))
        )
    
        val mismatch: Array[(StructField, StructField)] = schemaComplianceProfile.schemaDifference
        assert(expected.head._1 == mismatch.head._1)
        assert(expected.head._2 == mismatch.head._2)
    }
    
    test("SCHEMA: should be able to find the values that are not of expected type") {
        val spark = sparkSession
    
        val callRecord: AnalyzableDataset = new AnalyzableDataset(spark, "data/calls_with_header.csv", CSV)
        val callRecordSchemaProfile: SchemaComplianceProfile = callRecord.analyzeSchemaCompliance(CallRecord.getSchema)
        val reportForOther: FieldReport = callRecordSchemaProfile.reportFor("Other")
        
        assert(reportForOther.nonCompliantValues.schema.fields.head.name == "other")
        assert(11224 == reportForOther.nonCompliantValues.count)
        assert(LongType == reportForOther.actualDataType)
        assert(IntegerType == reportForOther.expectedDataType)
        assert("other" == reportForOther.actualFieldName)
        assert("Other" == reportForOther.expectedFieldName)
    }
    
    test("SCHEMA: should return empty dataframe when type matches but the column names are different") {
        val spark = sparkSession
        object CallRecord {
            private val user = StructField("user", LongType)
            private val other = StructField("Callee", LongType)
            private val direction = StructField("direction", StringType)
            private val duration = StructField("duration", IntegerType)
            private val timestamp = StructField("timestamp", StringType)
            
            def getSchema: StructType = StructType(Array(user, other, direction, duration, timestamp))
        }
        val callRecord: AnalyzableDataset = new AnalyzableDataset(spark, "data/calls_with_header.csv", CSV)
        
        val callRecordSchemaProfile: SchemaComplianceProfile = callRecord.analyzeSchemaCompliance(CallRecord.getSchema)
        val reportForOther: FieldReport = callRecordSchemaProfile.reportFor("Callee")
        
        assert(reportForOther.nonCompliantValues.schema.fields.head.name == "other")
        
        assert(reportForOther.actualDataType == LongType)
        assert(reportForOther.expectedDataType == LongType)
        assert(reportForOther.actualFieldName == "other")
        assert(reportForOther.expectedFieldName == "Callee")
    
        assert(0 == reportForOther.nonCompliantValues.count)
    }
    
    test("SCHEMA: should throw exception when original dataset schema has different number of fields than expected") {
        val spark = sparkSession
        object CallRecord {
            private val user = StructField("user", LongType)
            private val other = StructField("Callee", LongType)
            private val location = StructField("Callee", LongType)
            private val direction = StructField("direction", StringType)
            private val duration = StructField("duration", IntegerType)
            
            def getSchema: StructType = StructType(Array(user, other, direction, duration))
        }
        val callRecord: AnalyzableDataset = new AnalyzableDataset(spark, "data/calls_with_header.csv", CSV)
        
        
        val resultException: ApplicationException = intercept[ApplicationException] {
            callRecord.analyzeSchemaCompliance(CallRecord.getSchema)
        }
        assert("Number of fields must be same in the expected schema" == resultException.getMessage)
    }
    
    //    test("ROW_COMPLETENESS: should give percentage of row completeness when the all the columns are null") {
    //        val spark = sparkSession
    //
    //        val callRecord: AnalyzableDataset = new AnalyzableDataset(spark, "data/calls_with_header.csv", CSV)
    //
    //        val completenessRule: RowCompletenessRule = new RowCompletenessRule("empty" :: "-" :: Nil)
    //        val callRecordCompleteness: RowCompletenessProfile = callRecord.analyzeCompleteness(completenessRule)
    //
    //        assert(20 == callRecordCompleteness.percentage)
    //    }
}


