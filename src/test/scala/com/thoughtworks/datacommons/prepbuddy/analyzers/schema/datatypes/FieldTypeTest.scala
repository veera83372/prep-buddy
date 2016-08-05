package com.thoughtworks.datacommons.prepbuddy.analyzers.schema.datatypes

import com.thoughtworks.datacommons.prepbuddy.analyzers.schema.datatypes.FieldType.inferField
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class FieldTypeTest extends FunSuite {
    test("String fields types are inferred correctly") {
        assert(inferField("test") == StringType)
        assert(inferField("test") != IntegerType)
        assert(inferField("12-23-32") == StringType)
        assert(inferField("2015-08 14:49:00") == StringType)
        assert(inferField("2015-08-20 14") == StringType)
        assert(inferField("2015-08-20 14:10") == StringType)
    }
    
    test("Integer fields types are inferred correctly") {
        assert(inferField("60") == IntegerType)
        assert(inferField(" 01") == IntegerType)
        assert(inferField(" 01.5") != IntegerType)
    }
    
    test("Double fields types are inferred correctly") {
        assert(inferField("3.5") == DoubleType)
        assert(inferField("1.0") == DoubleType)
        assert(inferField("1.0") == DoubleType)
        assert(inferField("32.322") != LongType)
    }
    
    test("Long fields types are inferred correctly") {
        assert(inferField("100000000000") == LongType)
        assert(inferField("  492347943247   ") == LongType)
        assert(inferField("492347.943247") != LongType)
    }
    
    test("Null fields are handled properly") {
        assert(inferField(" null", "null") == NullType)
        assert(inferField("\\N", "\\N") == NullType)
        assert(inferField(" empty ", "empty") == NullType)
        assert(inferField(" \t \t ") == NullType)
        assert(inferField("") == NullType)
        assert(inferField(null) == NullType)
        assert(inferField("x") != NullType)
    }
    
    test("Boolean fields types are inferred correctly") {
        assert(inferField("False") == BooleanType)
        assert(inferField("True ") == BooleanType)
        assert(inferField("TRue") == BooleanType)
        assert(inferField(" FALSe ") == BooleanType)
        assert(inferField(" ") != BooleanType)
    }
    
    test("Timestamp fields are infered correctly") {
        assert(inferField("2015-08-20 14:57:00") == TimestampType)
        assert(inferField("2015-08-20 15:57:00") == TimestampType)
        assert(inferField("2015-08-20 15:57:00") != StringType)
    }
}
