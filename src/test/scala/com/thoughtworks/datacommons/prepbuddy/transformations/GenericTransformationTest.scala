package com.thoughtworks.datacommons.prepbuddy.transformations

import com.thoughtworks.datacommons.prepbuddy.SparkTestCase
import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.rdd.RDD

class GenericTransformationTest extends SparkTestCase {

    test("should be able to perform a operation and append a column based on IF condition") {
        val dataSet = Array(
            "true, standard deviation ,error ",
            "false, sinFunction, cosFunction",
            "False, logFunction,null",
            "xyz,variance,cosineFunction"
        )
        val initialRDD: RDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)

        val transformation: GenericTransformation = new LogicalTransformation().IF(0, 1, 2)
        val appendedColumnRDD: TransformableRDD = transformableRDD.appendNewColumn(transformation)

        val actual: Array[String] = appendedColumnRDD.collect()
        assert(actual(0).equals("true,standard deviation,error,standard deviation"))
        assert(actual(1).equals("false,sinFunction,cosFunction,cosFunction"))
        assert(actual(2).equals("False,logFunction,null,null"))
        assert(actual(3).equals("xyz,variance,cosineFunction,null"))
    }

    test("should be able to perform a operation and append a column based on AND condition") {
        val dataSet = Array(
            "true, true ,true ",
            "false, true, true",
            "False,false,false",
            "true,false,true",
            "xyz,a,b"
        )
        val initialRDD: RDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)

        val transformation: GenericTransformation = new LogicalTransformation().AND(0, 1, 2)
        val appendedColumnRDD: TransformableRDD = transformableRDD.appendNewColumn(transformation)

        val actual: Array[String] = appendedColumnRDD.collect()
        assert(actual(0).equals("true,true,true,true"))
        assert(actual(1).equals("false,true,true,false"))
        assert(actual(2).equals("False,false,false,false"))
        assert(actual(3).equals("true,false,true,false"))
        assert(actual(4).equals("xyz,a,b,null"))
    }

    test("should be able to perform a operation and append a column based on OR condition") {
        val dataSet = Array(
            "true, true ,true ",
            "false, true, true",
            "False,false,false",
            "true,false,true",
            "xyz,a,b"
        )
        val initialRDD: RDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)

        val transformation: GenericTransformation = new LogicalTransformation().OR(0, 1, 2)
        val appendedColumnRDD: TransformableRDD = transformableRDD.appendNewColumn(transformation)

        val actual: Array[String] = appendedColumnRDD.collect()
        assert(actual(0).equals("true,true,true,true"))
        assert(actual(1).equals("false,true,true,true"))
        assert(actual(2).equals("False,false,false,false"))
        assert(actual(3).equals("true,false,true,true"))
        assert(actual(4).equals("xyz,a,b,null"))
    }


}
