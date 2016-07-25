package org.apache.datacommons.prepbuddy.rdds

import org.apache.datacommons.prepbuddy.DFTestCase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class AnalyzableDFTest extends DFTestCase {

    object CallRecordSchema {
        private val user = StructField("User", LongType)
        private val other = StructField("Other", LongType)
        private val direction = StructField("Direction", StringType)
        private val duration = StructField("Duration", IntegerType)
        private val timestamp = StructField("Timestamp", StringType)

        def getSchema: StructType = StructType(Array(user, other, direction, duration, timestamp))
    }

    test("should determine the extent of missing value in the column") {
        val callRecords: RDD[Row] = sparkContext.parallelize(Array(
            Row(7434677419L, null, "Incoming", 211, "Wed Sep 15 19:17:44 +0100 2010"),
            Row(7641036117L, 1666472054L, "Outgoing", 0, "Mon Feb 11 07:18:23 +0000 1980"),
            Row(7641036117L, 7371326239L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            Row(7641036117L, 7371326239L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            Row(7641036117L, null, "Missed", 12, "Mon Feb 11 08:04:42 +0000 1980"),
            Row(7641036117L, 7371326239L, "Incoming", 4, "Mon Feb 11 07:45:42 +0000 1980"),
            Row(7641036117L, 7371326239L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            Row(7641036117L, 7681546436L, "Missed", 12, "Mon Feb 11 08:04:42 +0000 1980"),
            Row(7641036117L, 7371326239L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            Row(7641036117L, null, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            Row(7641036117L, null, "Outgoing", 421, "Mon Feb 11 07:45:42 +0000 1980"),
            Row(7641036117L, 7681546431L, "Missed", 12, "Mon Feb 11 08:04:42 +0000 1980"),
            Row(7641036117L, 7371326235L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            Row(7641036117L, 7371326236L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            Row(7641036117L, 7681546437L, "Missed", 12, "Mon Feb 11 08:04:42 +0000 1980"),
            Row(7641036117L, 7371326238L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            Row(7641036117L, 7371326230L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            Row(7641036117L, 7371326230L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            Row(7641036117L, 7371326230L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            Row(7641036117L, 7371326230L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980)")
        ))

        val callRecordsDF: DataFrame = sqlContext.createDataFrame(callRecords, CallRecordSchema.getSchema)
        val analyzableCallRecords: AnalyzableDF = new AnalyzableDF(callRecordsDF)

        assert(20.0 == analyzableCallRecords.percentageOfMissingValue("Other"))
    }

}
