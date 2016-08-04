package com.thoughtworks.datacommons.prepbuddy.analyzers.types

import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class FieldTypeTest extends FunSuite {
    test("String fields types are inferred correctly") {
        assert(FieldType.inferField("test") == StringType)
        assert(FieldType.inferField("test") != IntegerType)
        assert(FieldType.inferField("12-23-32") == StringType)
        assert(FieldType.inferField("2015-08 14:49:00") == StringType)
        assert(FieldType.inferField("2015-08-20 14") == StringType)
        assert(FieldType.inferField("2015-08-20 14:10") == StringType)
    }
    
    test("Integer fields types are inferred correctly") {
        assert(FieldType.inferField("60") == IntegerType)
        assert(FieldType.inferField(" 01") == IntegerType)
        assert(FieldType.inferField(" 01.5") != IntegerType)
    }
    
    test("Double fields types are inferred correctly") {
        assert(FieldType.inferField("3.5") == DoubleType)
        assert(FieldType.inferField("1.0") == DoubleType)
        assert(FieldType.inferField("1.0") == DoubleType)
        assert(FieldType.inferField("32.322") != LongType)
    }
    
    test("Long fields types are inferred correctly") {
        assert(FieldType.inferField("100000000000") == LongType)
        assert(FieldType.inferField("  492347943247   ") == LongType)
        assert(FieldType.inferField("492347.943247") != LongType)
    }
    
    test("Null fields are handled properly") {
        assert(FieldType.inferField(" null", "null") == NullType)
        assert(FieldType.inferField("\\N", "\\N") == NullType)
        assert(FieldType.inferField(" empty ", "empty") == NullType)
        assert(FieldType.inferField(" \t \t ") == NullType)
        assert(FieldType.inferField("") == NullType)
        assert(FieldType.inferField(null) == NullType)
        assert(FieldType.inferField("x") != NullType)
    }
    
    test("Boolean fields types are inferred correctly") {
        assert(FieldType.inferField("False") == BooleanType)
        assert(FieldType.inferField("True ") == BooleanType)
        assert(FieldType.inferField("TRue") == BooleanType)
        assert(FieldType.inferField(" FALSe ") == BooleanType)
        assert(FieldType.inferField(" ") != BooleanType)
    }
    
    test("Timestamp fields are infered correctly") {
        assert(FieldType.inferField("2015-08-20 14:57:00") == TimestampType)
        assert(FieldType.inferField("2015-08-20 15:57:00") == TimestampType)
        assert(FieldType.inferField("2015-08-20 15:57:00") != StringType)
    }
}
