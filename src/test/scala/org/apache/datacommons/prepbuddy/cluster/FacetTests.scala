package org.apache.datacommons.prepbuddy.cluster

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.datacommons.prepbuddy.clusterers.TextFacets
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.api.java.JavaRDD
import org.junit.Assert.assertTrue

class FacetTests extends SparkTestCase {

    test("should drop the specified column from the given rdd") {
        val dataSet = Array("X,Y", "A,B", "X,Z", "A,Q", "A,E")
        val initialRDD: JavaRDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)
        val textFaceted: TextFacets = transformableRDD.listFacets(0)
        val expected = ("A", 3)
        assert(textFaceted.highest.length == 1)
        val highest = textFaceted.highest.head

        assert(expected == highest)
    }

    test("highest should give one pair in List if only one lowest pair is found") {
        val dataSet = Array("X,Y", "A,B", "X,Z", "A,Q", "A,E", "X,P")
        val initialRDD: JavaRDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)
        val textFaceted: TextFacets = transformableRDD.listFacets(0)
        val listOfHighest = textFaceted.highest

        assert(2 == listOfHighest.length)
        assert(2 == textFaceted.count)

        assertTrue(listOfHighest.contains(("A", 3)))
        assertTrue(listOfHighest.contains(("X", 3)))
    }


    test("Textfacet  lowest should give list of lowest pairs if more than one pair found") {
        val dataSet = Array("X,Y", "A,B", "X,Z", "A,Q", "A,E", "Q,E", "Q,R")
        val initialRDD: JavaRDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)
        val textFaceted: TextFacets = transformableRDD.listFacets(0)
        val listOfLowest = textFaceted.lowest
        assert(3 == textFaceted.count)
        assert(2 == listOfLowest.length)
        assertTrue(listOfLowest.contains(("X", 2)))
        assertTrue(listOfLowest.contains(("Q", 2)))
    }

    test(" TextFacet_ get Facets Between Should Give List Of Faceted Pair In Given Range ") {
        val dataSet = Array("X,Y", "A,B", "X,Z", "A,Q", "A,E", "Q,E", "Q,R", "W,E")
        val initialRDD: JavaRDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)
        val textFaceted: TextFacets = transformableRDD.listFacets(0)
        val facetedPair = textFaceted.getFacetsBetween(2, 3)
        assert(4 == textFaceted.count)

        assert(3 == facetedPair.length)
        assertTrue(facetedPair.contains(("X", 2)))
        assertTrue(facetedPair.contains(("Q", 2)))
        assertTrue(facetedPair.contains(("A", 3)))
    }

    test(" cardinalValues  Should return cardinal values for the text facets "){
        val dataSet = Array("X,Y", "A,B", "X,Z", "A,Q", "A,E", "Q,E", "Q,R", "W,E")
        val initialRDD: JavaRDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)
        val textFaceted: TextFacets = transformableRDD.listFacets(0)
        val cardinalValues: Array[String] = textFaceted.cardinalValues

        assert(cardinalValues sameElements Array("Q", "A", "X", "W"))
    }
}
