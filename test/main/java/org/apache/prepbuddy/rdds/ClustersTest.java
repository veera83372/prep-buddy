package org.apache.prepbuddy.rdds;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.groupingops.*;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class ClustersTest extends SparkTestCase {
        @Test
    public void shouldGiveClusterOfSimilarColumnValues() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("CLUSTER Of Finger print", "finger print of cluster", "finger print for cluster"));
            TransformableRDD initialRDD = new TransformableRDD(initialDataset);
            Clusters clusters = initialRDD.clusters(0, new SimpleFingerprint());

        Tuple2<String, Integer> expected1 = new Tuple2<>("CLUSTER Of Finger print", 1);
        Tuple2<String, Integer> expected2 = new Tuple2<>("finger print of cluster", 1);
        Tuple2<String, Integer> expected3 = new Tuple2<>("finger print for cluster", 1);

        List<Cluster> listOfCluster = clusters.getAllClusters();
        assertEquals(2, listOfCluster.size());
        assertTrue(listOfCluster.get(0).contain(expected1));
        assertTrue(listOfCluster.get(0).contain(expected2));
        assertFalse(listOfCluster.get(0).contain(expected3));
    }

    @Test
    public void clusterByNGramFingerPrintShouldGiveClustersByNGramMethod() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("CLUSTER Of Finger print", "finger print of cluster", "finger print for cluster"));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        Clusters clusters = initialRDD.clusters(0, new NGramFingerprint(1));

        List<Cluster> clustersWithSizeGreaterThanOne = clusters.getClustersWithSizeGreaterThan(2);
        assertEquals(1, clustersWithSizeGreaterThanOne.size());
        assertEquals(3, clustersWithSizeGreaterThanOne.get(0).size());

    }



    @Test
    public void clusterUsingLevenshteinDistanceShouldGiveClustersByDistanceMethod() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("cluster Of Finger print", "finger print of cluster", "finger print for cluster"));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        Clusters clusters = initialRDD.clusters(0, new LevenshteinDistance());

        List<Cluster> clustersWithSizeGreaterThanOne = clusters.getClustersWithSizeGreaterThan(1);
        assertEquals(1, clustersWithSizeGreaterThanOne.size());
        assertEquals(2, clustersWithSizeGreaterThanOne.get(0).size());

    }
}
