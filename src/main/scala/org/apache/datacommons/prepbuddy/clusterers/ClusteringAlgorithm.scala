package org.apache.datacommons.prepbuddy.clusterers


trait ClusteringAlgorithm {
    def getClusters(tuples: Array[(String, Int)]): Clusters
}
