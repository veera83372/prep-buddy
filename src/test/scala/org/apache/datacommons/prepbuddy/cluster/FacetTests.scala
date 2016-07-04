package org.apache.datacommons.prepbuddy.cluster

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.datacommons.prepbuddy.clusterers.TextFacets
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.api.java.JavaRDD
import org.junit.Assert.assertTrue

class FacetTests extends SparkTestCase {

    test("should drop the specified column from the given rdd") {
        val initialDataset: JavaRDD[String] = sparkContext.parallelize(Array("X,Y", "A,B", "X,Z", "A,Q", "A,E"))
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val textFaceted: TextFacets = initialRDD.listFacets(0)
        val expected = ("A", 3)
        assert(textFaceted.highest.size == 1)
        val highest = textFaceted.highest.head

        assert(expected == highest)
    }

    test("highest should give one pair in List if only one lowest pair is found") {
        val initialDataset: JavaRDD[String] = sparkContext.parallelize(Array("X,Y", "A,B", "X,Z", "A,Q", "A,E", "X,P"))
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val textFaceted: TextFacets = initialRDD.listFacets(0)
        val listOfHighest = textFaceted.highest

        assert(2 == listOfHighest.size)
        assert(2 == textFaceted.count)

        assertTrue(listOfHighest.contains(("A", 3)))
        assertTrue(listOfHighest.contains(("X", 3)))
    }


    test("Textfacet  lowest should give list of lowest pairs if more than one pair found") {
        val initialDataset = sparkContext.parallelize(Array("X,Y", "A,B", "X,Z", "A,Q", "A,E", "Q,E", "Q,R"))
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val textFaceted: TextFacets = initialRDD.listFacets(0)
        val listOfLowest = textFaceted.lowest
        assert(3 == textFaceted.count)
        assert(2 == listOfLowest.size)
        assertTrue(listOfLowest.contains(("X", 2)))
        assertTrue(listOfLowest.contains(("Q", 2)))
    }

    test(" TextFacet_ get Facets Between Should Give List Of Faceted Pair In Given Range ") {
        val initialDataset = sparkContext.parallelize(Array("X,Y", "A,B", "X,Z", "A,Q", "A,E", "Q,E", "Q,R", "W,E"))
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val textFaceted: TextFacets = initialRDD.listFacets(0)
        val facetedPair = textFaceted.getFacetsBetween(2, 3)
        assert(4 == textFaceted.count)

        assert(3 == facetedPair.size)
        assertTrue(facetedPair.contains(("X", 2)))
        assertTrue(facetedPair.contains(("Q", 2)))
        assertTrue(facetedPair.contains(("A", 3)))
    }

}
