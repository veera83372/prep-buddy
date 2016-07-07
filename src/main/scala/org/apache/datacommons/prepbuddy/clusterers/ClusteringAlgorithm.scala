package org.apache.datacommons.prepbuddy.clusterers


trait ClusteringAlgorithm extends Serializable {
    def getClusters(tuples: Array[(String, Int)]): Clusters
}
