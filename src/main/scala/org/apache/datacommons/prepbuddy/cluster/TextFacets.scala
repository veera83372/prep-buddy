package org.apache.datacommons.prepbuddy.cluster

import org.apache.spark.rdd.RDD

class TextFacets(facets: RDD[(String, Int)]) {
    def count: Long = facets.count
}
