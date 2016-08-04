package org.datacommons.prepbuddy.analyzers.types

import java.sql.Timestamp

import org.apache.spark.sql.types._

import scala.util.control.Exception._

object FieldType {
    def inferField(field: String, nullValue: String = ""): DataType = {
        
        def tryParseInteger(field: String): DataType = if ((allCatch opt field.toInt).isDefined) {
            IntegerType
        } else {
            tryParseLong(field)
        }
        
        def tryParseLong(field: String): DataType = if ((allCatch opt field.toLong).isDefined) {
            LongType
        } else {
            tryParseDouble(field)
        }
        
        def tryParseDouble(field: String): DataType = {
            if ((allCatch opt field.toDouble).isDefined) {
                DoubleType
            } else {
                tryParseTimestamp(field)
            }
        }
        
        def tryParseTimestamp(field: String): DataType = {
            if ((allCatch opt Timestamp.valueOf(field)).isDefined) {
                TimestampType
            } else {
                tryParseBoolean(field)
            }
        }
        
        def tryParseBoolean(field: String): DataType = {
            if ((allCatch opt field.toBoolean).isDefined) {
                BooleanType
            } else {
                StringType
            }
        }
        
        def tryParsing(field: String) = tryParseInteger(field.trim)
        
        if (field == null || field.isEmpty || field.trim == nullValue) NullType else tryParsing(field.trim)
    }
}
