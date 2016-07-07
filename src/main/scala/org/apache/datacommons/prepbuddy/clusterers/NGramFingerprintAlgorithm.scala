package org.apache.datacommons.prepbuddy.clusterers

import scala.collection.mutable

class NGramFingerprintAlgorithm(nGram: Int) extends FingerprintAlgorithm {
    override def getClusters(tuples: Array[(String, Int)]): Clusters = {
        val clusters: Clusters = new Clusters
        tuples.foreach((tuple) => {
            val key: String = generateNGramFingerprint(tuple._1)
            clusters.add(key, tuple)
        })
        clusters
    }

    def generateNGramFingerprint(string: String): String = {
        val someString: String = removeAllPunctuations(string.trim.toLowerCase)
        val set: mutable.TreeSet[String] = getNGramSetOf(someString)
        set.mkString("")
    }

    private def getNGramSetOf(someString: String): mutable.TreeSet[String] = {
        val set: mutable.TreeSet[String] = new mutable.TreeSet[String]()
        val charArray: Array[Char] = someString.toCharArray
        var index = 0
        while (index + nGram <= charArray.length) {
            set.add(new String(charArray, index, nGram))
            index += 1
        }
        set
    }
}
