package org.datacommons.prepbuddy.analyzers

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.datacommons.prepbuddy.types.CSV
import org.scalatest.FunSuite

//07610039694,07434677419,Incoming,211,Wed Sep 15 19:17:44 +0100 2010
case class CallDataRecord(caller: String, callee: String, callType: String,
                          callDuration: Long, callInitiatedAt: String)

class AnalyzableDatasetTest extends FunSuite {


    ignore("shouldValidateSchema") {
        val sparkSession: SparkSession = SparkSession.builder().
            master("local[2]").appName(getClass.getCanonicalName).getOrCreate()
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
                StructField("a", IntegerType, true) ::
                    StructField("b", LongType, false) ::
                    StructField("c", BooleanType, false) :: Nil)

        val schemaComplianceProfile = analyzableDataset.analyzeSchemaCompliance(struct)

        val completenessProfile = analyzableDataset.analyzeCompleteness(new RowCompletenessRule())

        val columnProfile = analyzableDataset.analyzeColumn("column-name", new ColumnRules())

    }


}


