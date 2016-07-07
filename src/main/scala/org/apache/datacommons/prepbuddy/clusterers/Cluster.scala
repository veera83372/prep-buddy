package org.apache.datacommons.prepbuddy.clusterers

class Cluster(key: String) extends Serializable {

    var tuples: List[(String, Int)] = List()

    def contain(tuple: (String, Int)): Boolean = tuples.contains(tuple)

    def size: Int = tuples.size

    def isOfKey(key: String): Boolean = key.equals(this.key)

    def containsValue(value: String): Boolean = tuples.exists(_._1 == value)

    def add(tuple: (String, Int)): Unit = tuples = tuples.:+(tuple)

}
