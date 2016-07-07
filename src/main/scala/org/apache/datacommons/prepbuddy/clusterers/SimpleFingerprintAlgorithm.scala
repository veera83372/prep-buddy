package org.apache.datacommons.prepbuddy.clusterers

import org.apache.commons.lang.StringUtils

import scala.collection.mutable

class SimpleFingerprintAlgorithm extends FingerprintAlgorithm with Serializable {

    override def getClusters(tuples: Array[(String, Int)]): Clusters = {
        val clusters: Clusters = new Clusters
        tuples.foreach((tuple) => {
            val key: String = generateSimpleFingerprint(tuple._1)
            clusters.add(key, tuple)
        })
        clusters
    }

    def generateSimpleFingerprint(string: String): String = {
        val fragments: Array[String] = StringUtils.split(removeAllPunctuations(string.trim.toLowerCase))
        rearrangeAlphabetically(fragments)
    }

    def rearrangeAlphabetically(fragments: Array[String]): String = {
        var set: mutable.TreeSet[String] = new mutable.TreeSet()
        set = set ++ fragments
        set.mkString(" ")
    }
}
