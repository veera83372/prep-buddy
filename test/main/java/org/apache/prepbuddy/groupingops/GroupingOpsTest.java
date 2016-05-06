package org.apache.prepbuddy.groupingops;

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

public class GroupingOpsTest extends SparkTestCase{
    @Test
    public void _TextFacetShouldGiveCountOfPair() {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("X,Y", "A,B", "X,Z","A,Q","A,E"));
        TextFacets textFacets = GroupingOps.listTextFacets(initialDataset, 0, FileType.CSV);
        assertEquals(2, textFacets.count());
    }

    @Test
    public void _TextFacets_highestShouldGiveOneHighestPairIfOnlyOnePairFound() throws Exception {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("X,Y", "A,B", "X,Z","A,Q","A,E"));
        TextFacets textFaceted = GroupingOps.listTextFacets(initialDataset, 0, FileType.CSV);

        Tuple2<String, Integer> expected = new Tuple2<>("A", 3);
        List<Tuple2> listOfHighest = textFaceted.highest();

        assertEquals(1, listOfHighest.size());

        Tuple2 actual = listOfHighest.get(0);
        assertEquals(2,textFaceted.count());
        assertEquals(expected, actual);
    }

    @Test
    public void _TextFacet_highestShouldGiveListOfHighestPairsIfMoreThanOnePairFound() throws Exception {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("X,Y", "A,B", "X,Z","A,Q","A,E","X,P"));

        TextFacets textFaceted = GroupingOps.listTextFacets(initialDataset, 0, FileType.CSV);
        List<Tuple2> listOfHighest = textFaceted.highest();

        assertEquals(2, listOfHighest.size());
        assertEquals(2,textFaceted.count());

        Tuple2<String, Integer> expected1 = new Tuple2<>("A", 3);
        Tuple2<String, Integer> expected2 = new Tuple2<>("X", 3);

        assertTrue(listOfHighest.contains(expected1));
        assertTrue(listOfHighest.contains(expected2));
    }
    @Test
    public void _TextFacet_lowestShouldGiveOnePairInListIfOnlyOneLowestPairIsFound() throws Exception {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("X,Y", "A,B", "X,Z","A,Q","A,E"));

        TextFacets textFaceted = GroupingOps.listTextFacets(initialDataset, 0, FileType.CSV);

        List<Tuple2> listOfLowest = textFaceted.lowest();

        assertEquals(1, listOfLowest.size());

        Tuple2<String, Integer> expected = new Tuple2<>("X", 2);
        Tuple2 actual = listOfLowest.get(0);

        assertEquals(2, textFaceted.count());
        assertEquals(expected, actual);
    }

    @Test
    public void _TextFacet_lowestShouldGiveListOfLowestPairsIfMoreThanOnePairFound() throws Exception {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("X,Y", "A,B", "X,Z","A,Q","A,E","Q,E","Q,R"));

        TextFacets textFaceted = GroupingOps.listTextFacets(initialDataset, 0, FileType.CSV);

        List<Tuple2> listOfLowest = textFaceted.lowest();
        assertEquals(3, textFaceted.count());
        assertEquals(2, listOfLowest.size());

        Tuple2<String, Integer> expected1 = new Tuple2<>("X", 2);
        Tuple2<String, Integer> expected2 = new Tuple2<>("Q", 2);

        assertTrue(listOfLowest.contains(expected1));
        assertTrue(listOfLowest.contains(expected2));

    }
    @Test
    public void _TextFacet_getFacetsBetweenShouldGiveListOfFacetedPairInGivenRange() throws Exception {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("X,Y", "A,B", "X,Z","A,Q","A,E","Q,E","Q,R","W,E"));

        TextFacets textFaceted = GroupingOps.listTextFacets(initialDataset, 0, FileType.CSV);

        List<Tuple2> facetedPair = textFaceted.getFacetsBetween(2,3);

        assertEquals(4, textFaceted.count());
        assertEquals(3, facetedPair.size());

        Tuple2<String, Integer> expected1 = new Tuple2<>("X", 2);
        Tuple2<String, Integer> expected2 = new Tuple2<>("Q", 2);
        Tuple2<String, Integer> expected3 = new Tuple2<>("A", 3);

        assertTrue(facetedPair.contains(expected1));
        assertTrue(facetedPair.contains(expected2));
        assertTrue(facetedPair.contains(expected3));
    }

    @Test
    public void shouldGiveClusterOfSimilarColumnValues() {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("CLUSTER Of Finger print", "finger print of cluster", "finger print for cluster"));
        Clusters clusters = GroupingOps.clusterUsingSimpleFingerprint(initialDataset, 0, FileType.CSV);

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
    public void shouldGiveNumberOfClusters() {
        JavaRDD<String> dataset = context.textFile("data/phm_collection.txt");
        Clusters clusters = GroupingOps.clusterUsingSimpleFingerprint(dataset, 8, FileType.TSV);

        List<Cluster> duplicateClusters = clusters.getClustersWithSizeGreaterThan(1);
        assertEquals(35, duplicateClusters.size());
    }

    @Test
    public void clusterByNGramFingerPrintShouldGiveClustersByNGramMethod() {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("CLUSTER Of Finger print", "finger print of cluster", "finger print for cluster"));
        Clusters clusters = GroupingOps.clusterUsingNGramFingerprint(initialDataset, 0, FileType.CSV,1);

        List<Cluster> clustersWithSizeGreaterThanOne = clusters.getClustersWithSizeGreaterThan(2);
        assertEquals(1, clustersWithSizeGreaterThanOne.size());
        assertEquals(3, clustersWithSizeGreaterThanOne.get(0).size());

    }

    @Test
    public void clusterByNGramFingerPrintShouldGiveNumberOfClusters() {
        JavaRDD<String> dataset = context.textFile("data/newCollection.txt");
        Clusters clusters = GroupingOps.clusterUsingNGramFingerprint(dataset, 8, FileType.TSV,2);

        List<Cluster> duplicateClusters = clusters.getClustersWithSizeGreaterThan(1);
        assertEquals(114, duplicateClusters.size());
    }

    @Test
    public void clusterUsingLevenshteinDistanceShouldGiveClustersByDistanceMethod() {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("cluster Of Finger print", "finger print of cluster", "finger print for cluster"));
        Clusters clusters = GroupingOps.clusterUsingLevenshteinDistance(initialDataset, 0, FileType.CSV);

        List<Cluster> clustersWithSizeGreaterThanOne = clusters.getClustersWithSizeGreaterThan(1);
        assertEquals(1, clustersWithSizeGreaterThanOne.size());
        assertEquals(2, clustersWithSizeGreaterThanOne.get(0).size());

    }
}