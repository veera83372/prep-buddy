package org.apache.datacommons.prepbuddy.clusterers

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.rdd.RDD

class ClusterTest extends SparkTestCase {
    test("should Give Cluster Of Similar Column Values") {
        val dataSet = Array("CLUSTER Of Finger print", "finger print of cluster", "finger print for cluster")
        val initialRDD: RDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)
        val clusters: Clusters = transformableRDD.clusters(0, new SimpleFingerprintAlgorithm)

        val listOfCluster: List[Cluster] = clusters.getAllClusters
        assert(2 == listOfCluster.size)
        assert(listOfCluster.head.contain(("CLUSTER Of Finger print", 1)))
        assert(listOfCluster.head.contain(("finger print of cluster", 1)))

        assert(!listOfCluster.head.contain(("finger print for cluster", 1)))
    }

    test("cluster By NGramFingerPrint Should Give Clusters By NGramMethod") {
        val dataSet = Array("CLUSTER Of Finger print", "finger print of cluster", "finger print for cluster")
        val initialDataset: RDD[String] = sparkContext.parallelize(dataSet)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val clusters: Clusters = initialRDD.clusters(0, new NGramFingerprintAlgorithm(1))
        val clustersWithSizeGreaterThanOne: List[Cluster] = clusters.getClustersWithSizeGreaterThan(2)
        assert(1 == clustersWithSizeGreaterThanOne.size)
        assert(3 == clustersWithSizeGreaterThanOne.head.size)
    }

    test("clusterUsing LevenshteinDistance Should Give Clusters ByDistanceMethod") {
        val dataSet = Array("cluster Of Finger print", "finger print of cluster", "finger print for cluster")
        val initialRDD: RDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)
        val clusters: Clusters = transformableRDD.clusters(0, new LevenshteinDistance)
        val clustersWithSizeGreaterThanOne: List[Cluster] = clusters.getClustersWithSizeGreaterThan(1)
        assert(1 == clustersWithSizeGreaterThanOne.size)
        assert(2 == clustersWithSizeGreaterThanOne.head.size)
    }
}
