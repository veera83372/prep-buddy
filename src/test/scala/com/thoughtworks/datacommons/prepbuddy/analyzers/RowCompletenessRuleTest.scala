package com.thoughtworks.datacommons.prepbuddy.analyzers

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
            Row("John", "", "", 28, true),
            Row("Stiven", "", "Smith", 23, true),
            Row("Prasun", "Kumar", "Pal", 20, true),
            Row("Ram", null, "Lal", 50, true)
        )
        val personData: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), Person.getSchema)
        personData.collect()
    }
    
    private val persons: Array[Row] = getPersonsWithSchema
    
    private val john = persons(0)
    private val stiven = persons(1)
    private val prasun = persons(2)
    private val ram = persons(3)
    
    test("should return true when the specified column values is/are null") {
        val completenessRule: RowCompletenessRule = new RowCompletenessRule("empty" :: "-" :: Nil)
        completenessRule.incompleteWhenNullAt("middleName", "lastName")
        
        
        assert(!completenessRule.isComplete(john))
        assert(!completenessRule.isComplete(stiven))
        assert(!completenessRule.isComplete(ram))
        
        assert(completenessRule.isComplete(prasun))
    }
}
