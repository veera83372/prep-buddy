package org.apache.datacommons.prepbuddy.api.java;

import org.apache.datacommons.prepbuddy.api.JavaSparkTestCase;
import org.apache.datacommons.prepbuddy.api.java.types.FileType;
import org.apache.datacommons.prepbuddy.clusterers.TextFacets;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JavaFacetTest extends JavaSparkTestCase {
    @Test
    public void _TextFacetShouldGiveCountOfPair() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y", "A,B", "X,Z", "A,Q", "A,E"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        TextFacets textFacets = initialRDD.listFacets(0);
        assertEquals(2, textFacets.count());
    }

    @Test
    public void _TextFacets_highestShouldGiveOneHighestPairIfOnlyOnePairFound() throws Exception {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y", "A,B", "X,Z", "A,Q", "A,E"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        TextFacets textFaceted = initialRDD.listFacets(0);

        Tuple2<String, Integer> expected = new Tuple2<>("A", 3);
        Tuple2[] listOfHighest = textFaceted.highest();

        assertEquals(1, listOfHighest.length);

        Tuple2 actual = listOfHighest[0];
        assertEquals(2, textFaceted.count());
        assertEquals(expected, actual);
    }

    @Test
    public void _TextFacet_highestShouldGiveListOfHighestPairsIfMoreThanOnePairFound() throws Exception {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y", "A,B", "X,Z", "A,Q", "A,E", "X,P"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        TextFacets textFaceted = initialRDD.listFacets(0);
        Tuple2[] listOfHighest = textFaceted.highest();

        assertEquals(2, listOfHighest.length);
        assertEquals(2, textFaceted.count());

        Tuple2<String, Integer> expected1 = new Tuple2<>("A", 3);
        Tuple2<String, Integer> expected2 = new Tuple2<>("X", 3);

        assertTrue(listOfHighest[0].equals(expected1));
        assertTrue(listOfHighest[1].equals(expected2));
    }
}
