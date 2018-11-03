package com.thoughtworks.datacommons.prepbuddy.analyzers.completeness

import com.thoughtworks.datacommons.prepbuddy.analyzers.completeness.EvaluationMode.CUSTOM
import com.thoughtworks.datacommons.prepbuddy.exceptions.{ApplicationException, ErrorMessages}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.FunSuite

class RowCompletenessRuleTest extends FunSuite {
    private val spark: SparkSession = SparkSession.builder()
            .appName(getClass.getCanonicalName)
            .master("local[2]")
            .getOrCreate()
    
    
    def getPersonsWithSchema: Array[Row] = {
        object Person {
            private val firstName = StructField("firstName", StringType)
            private val middleName = StructField("middleName", StringType)
            private val lastName = StructField("lastName", StringType)
            private val age = StructField("age", IntegerType)
            private val married = StructField("married", BooleanType)
            
            def getSchema: StructType = StructType(Array(firstName, middleName, lastName, age, married))
        }
        
        val data: List[Row] = List(
            Row("John", "-", "-", 28, true),
            Row("Stiven", "", "Smith", null, true),
            Row("Prasun", "Kumar", "Pal", 20, true),
            Row("Ram", "Lal", "Panwala", null, true),
            Row("Babu", "Lal", "Phoolwala", 57, null)
        )
        val personData: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), Person.getSchema)
        personData.collect()
    }
    
    private val persons: Array[Row] = getPersonsWithSchema
    
    private val john = persons(0)
    private val stiven = persons(1)
    private val prasun = persons(2)
    private val ramlal = persons(3)
    private val babulal = persons(4)
    
    test("should return false when the specified column values is/are null") {
        val completenessRule: RowCompletenessRule = new RowCompletenessRule(
            CUSTOM,
            List("middleName", "age"),
            List("N/A", "-")
        )
        
        assert(!completenessRule.isComplete(john))
        assert(!completenessRule.isComplete(stiven))
        assert(!completenessRule.isComplete(ramlal))
        
        assert(completenessRule.isComplete(prasun))
        assert(completenessRule.isComplete(babulal))
    }
    
    test("should return false when any of the column value is null") {
        val completenessRule: RowCompletenessRule = new RowCompletenessRule(
            EvaluationMode.STRICT,
            possibleNullValues = List("empty", "-")
        )
        
        assert(!completenessRule.isComplete(john))
        assert(!completenessRule.isComplete(stiven))
        assert(!completenessRule.isComplete(ramlal))
        assert(!completenessRule.isComplete(babulal))
        
        assert(completenessRule.isComplete(prasun))
    }
    
    test("should throw exception when Evaluation mode is CUSTOM but mandatory columns are not specified") {
        val resultException: ApplicationException = intercept[ApplicationException] {
            new RowCompletenessRule(
                EvaluationMode.CUSTOM,
                possibleNullValues = List("empty", "-", "N/A")
            )
        }
        assert(ErrorMessages.REQUIREMENT_NOT_MATCHED.getMessage == resultException.getMessage)
    }
    
    test("should throw column not found exception when a mandatory column is not present") {
        val resultException: ApplicationException = intercept[ApplicationException] {
            new RowCompletenessRule(
                EvaluationMode.CUSTOM,
                mandatoryColumns = List("foo"),
                possibleNullValues = List("empty", "-", "N/A")
            ).isComplete(prasun)
        }
        
        assert(ErrorMessages.COLUMN_NOT_FOUND.getMessage == resultException.getMessage)
    }
}
