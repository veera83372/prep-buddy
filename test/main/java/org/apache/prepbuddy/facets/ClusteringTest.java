package org.apache.prepbuddy.facets;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class ClusteringTest extends SparkTestCase{
    @Test
    public void shouldGiveClusterOfSimilarColumnValues() {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("CLUSTER Of Finger print","finger print of cluster","finger print for cluster"));
        Clusters clusters = Clustering.byFingerPrint(initialDataset, 0, FileType.CSV);

        Tuple2<String, Integer> expected1 = new Tuple2<>("CLUSTER Of Finger print", 1);
        Tuple2<String, Integer> expected2 = new Tuple2<>("finger print of cluster", 1);
        Tuple2<String, Integer> expected3 = new Tuple2<>("finger print for cluster", 1);

        List<Cluster> listOfCluster = clusters.getClusters();
        assertEquals(2, listOfCluster.size());
        assertTrue(listOfCluster.get(0).contain(expected1));
        assertTrue(listOfCluster.get(0).contain(expected2));
        assertFalse(listOfCluster.get(0).contain(expected3));
    }
}