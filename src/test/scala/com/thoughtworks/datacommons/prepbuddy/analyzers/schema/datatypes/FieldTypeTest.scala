package com.thoughtworks.datacommons.prepbuddy.analyzers.schema.datatypes

import com.thoughtworks.datacommons.prepbuddy.analyzers.schema.datatypes.FieldType.inferField
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class FieldTypeTest extends FunSuite {
    test("String fields types are inferred correctly") {
        assert(StringType == inferField("test"))
        assert(IntegerType != inferField("test"))
        assert(StringType == inferField("12-23-32"))
        assert(StringType == inferField("2015-08 14:49:00"))
        assert(StringType == inferField("2015-08-20 14"))
        assert(StringType == inferField("2015-08-20 14:10"))
    }
    
    test("Integer fields types are inferred correctly") {
        assert(IntegerType == inferField("60"))
        assert(IntegerType == inferField(" 01"))
        assert(IntegerType != inferField(" 01.5"))
    }
    
    test("Double fields types are inferred correctly") {
        assert(DoubleType == inferField("3.5"))
        assert(DoubleType == inferField("1.0"))
        assert(DoubleType == inferField("1.0"))
        assert(LongType != inferField("32.322"))
    }
    
    test("Long fields types are inferred correctly") {
        assert(LongType == inferField("100000000000"))
        assert(LongType == inferField("  492347943247   "))
        assert(LongType != inferField("492347.943247"))
    }
    
    test("Null fields are handled properly") {
        assert(NullType == inferField(" null", "null"))
        assert(NullType == inferField("\\N", "\\N"))
        assert(NullType == inferField(" empty ", "empty"))
        assert(NullType == inferField(" \t \t "))
        assert(NullType == inferField(""))
        assert(NullType == inferField(null))
        assert(NullType != inferField("x"))
    }
    
    test("Boolean fields types are inferred correctly") {
        assert(BooleanType == inferField("False"))
        assert(BooleanType == inferField("True "))
        assert(BooleanType == inferField("TRue"))
        assert(BooleanType == inferField(" FALSe "))
        assert(BooleanType != inferField(" "))
    }
    
    test("Timestamp fields are infered correctly") {
        assert(TimestampType == inferField("2015-08-20 14:57:00"))
        assert(TimestampType == inferField("2015-08-20 15:57:00"))
        assert(StringType != inferField("2015-08-20 15:57:00"))
    }
}
