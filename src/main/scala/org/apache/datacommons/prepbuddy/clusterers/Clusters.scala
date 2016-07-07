package org.apache.datacommons.prepbuddy.clusterers

class Clusters {
    private var clusters: List[Cluster] = List()

    def getAllClusters: List[Cluster] = clusters

    def getClustersWithSizeGreaterThan(threshold: Int): List[Cluster] = {
        clusters.filter(_.size > threshold)
    }

    def getClustersExactlyOfSize(size: Int): List[Cluster] = {
        clusters.filter(_.size equals size)
    }

    def add(key: String, tuple: (String, Int)): Unit = {
        val option: Option[Cluster] = getClusterOf(key)
        var cluster: Cluster = option.orNull
        if (cluster == null) {
            cluster = new Cluster(key)
            clusters = clusters.:+(cluster)
        }
        cluster.add(tuple)
    }

    private def getClusterOf(key: String): Option[Cluster] = {
        clusters.find(_.isOfKey(key))
    }
}
