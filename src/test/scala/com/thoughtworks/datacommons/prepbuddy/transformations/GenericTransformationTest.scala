package com.thoughtworks.datacommons.prepbuddy.transformations

import com.thoughtworks.datacommons.prepbuddy.SparkTestCase
import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.rdd.RDD

class GenericTransformationTest extends SparkTestCase {

    test("should be able to perform a operation and append a column based on if condition") {
        val dataSet = Array(
            "true, standard deviation ,error ",
            "false, sinFunction, cosFunction",
            "False, logFunction,null",
            ",variance,cosineFunction"
        )
        val initialRDD: RDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)
        val appendedColumnRDD: TransformableRDD = transformableRDD.appendNewColumn(new LogicalTransformation())
        val actual: Array[String] = appendedColumnRDD.collect()
        assert(actual(0).equals("true,standard deviation,error,standard deviation"))
        assert(actual(1).equals("false,sinFunction,cosFunction,cosFunction"))
        assert(actual(2).equals("False,logFunction,null,null"))
        assert(actual(3).equals(",variance,cosineFunction,"))
    }

}
