package org.apache.datacommons.prepbuddy.cluster


import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.datacommons.prepbuddy.clusterers.TextFacets
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.api.java.JavaRDD

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
}
