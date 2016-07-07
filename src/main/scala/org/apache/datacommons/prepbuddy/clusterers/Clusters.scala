package org.apache.datacommons.prepbuddy.clusterers

class Clusters {
    private var clusters: List[Cluster] = List()

    def getAllClusters: List[Cluster] = clusters

    def getClustersWithSizeGreaterThan(threshold: Int): List[Cluster] = {
        var list: List[Cluster] = List()
        for (cluster <- clusters) {
            if (cluster.size > threshold) {
                list = list.:+(cluster)
            }
        }
        list
    }

    def getClustersExactlyOfSize(size: Int): List[Cluster] = {
        var list: List[Cluster] = List()
        for (cluster <- clusters) {
            if (cluster.size == size) {
                list = list.:+(cluster)
            }
        }
        list
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
