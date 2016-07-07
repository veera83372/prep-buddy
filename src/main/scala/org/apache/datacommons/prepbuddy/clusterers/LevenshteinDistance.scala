package org.apache.datacommons.prepbuddy.clusterers

import org.apache.commons.lang.StringUtils

class LevenshteinDistance extends ClusteringAlgorithm {
    override def getClusters(tuples: Array[(String, Int)]): Clusters = {
        val clusters: Clusters = new Clusters
        var indexes: List[Int] = List()
        tuples.zipWithIndex.foreach {
            case (tuple, index) =>
                val key: String = tuple._1
                clusters.add(key, tuple)
                var secondIndex: Int = index + 1
                while (secondIndex < tuples.length) {
                    val anotherTuple = tuples(secondIndex)
                    val anotherKey: String = anotherTuple._1
                    val distance: Int = StringUtils.getLevenshteinDistance(key, anotherKey)
                    if (distance < 4 && !indexes.contains(secondIndex)) {
                        clusters.add(key, anotherTuple)
                        indexes = indexes.:+(secondIndex)
                    }
                    secondIndex += 1
                }
        }
        clusters
    }
}


