package com.thoughtworks.datacommons.prepbuddy.analyzers

import com.thoughtworks.datacommons.prepbuddy.types.CSV
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

//07610039694,07434677419,Incoming,211,Wed Sep 15 19:17:44 +0100 2010
case class CallDataRecord(caller: String, callee: String, callType: String,
                          callDuration: Long, callInitiatedAt: String)

class AnalyzableDatasetTest extends FunSuite {

    ignore("shouldValidateSchema") {
        val sparkSession: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName(getClass.getCanonicalName)
            .getOrCreate()
        
        import sparkSession.implicits._
        val dataset: Dataset[String] = sparkSession.read.text("data/calls.csv").as[String]
        //        val callsDataset: Dataset[CallDataRecord] = dataset.map((record: String) => {
        //            val columns: Array[String] = record.split(",")
        //            CallDataRecord(columns(0), columns(1), columns(2), columns(3).toLong, columns(4))
        //        })
        //        callsDataset.printSchema()

        val analyzableDataset: AnalyzableDataset = new AnalyzableDataset(sparkSession, "data/calls.csv", CSV)
        val struct =
            StructType(
                StructField("a", IntegerType, nullable = true) ::
                    StructField("b", LongType, nullable = false) ::
                    StructField("c", BooleanType, nullable = false) :: Nil)
        
        val schemaComplianceProfile = analyzableDataset.analyzeSchemaCompliance(struct)

        val completenessProfile = analyzableDataset.analyzeCompleteness(new RowCompletenessRule())

        val columnProfile = analyzableDataset.analyzeColumn("column-name", new ColumnRules())
        sparkSession.stop()
    }

    test("should be able to find mismatches between two schema") {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName(getClass.getCanonicalName)
            .getOrCreate()

        object CallRecord {
            private val user = StructField("user", LongType)
            private val other = StructField("Other", LongType)
            private val direction = StructField("direction", StringType)
            private val duration = StructField("duration", IntegerType)
            private val timestamp = StructField("timestamp", StringType)

            def getSchema: StructType = StructType(Array(user, other, direction, duration, timestamp))
        }

        val callRecord: AnalyzableDataset = new AnalyzableDataset(spark, "data/calls_with_header.csv", CSV)
        val callRecordSchemaProfile: SchemaComplianceProfile = callRecord.analyzeSchemaCompliance(CallRecord.getSchema)

        val expected: Array[(StructField, StructField)] = Array(
            (StructField("Other", LongType), StructField("other", DecimalType.USER_DEFAULT))
        )

        val mismatch: Array[(StructField, StructField)] = callRecordSchemaProfile.missmatches
        assert(mismatch.head._1 == expected.head._1)
        assert(mismatch.head._2 == expected.head._2)

        spark.stop()
    }
    
}


