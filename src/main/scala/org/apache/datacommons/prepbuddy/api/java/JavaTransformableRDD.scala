package org.apache.datacommons.prepbuddy.api.java

import org.apache.datacommons.prepbuddy.clusterers.ClusteringAlgorithm
import org.apache.datacommons.prepbuddy.imputations.ImputationStrategy
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.smoothers.SmoothingMethod
import org.apache.datacommons.prepbuddy.types.FileType
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

class JavaTransformableRDD(rdd: JavaRDD[String], fileType: FileType) extends JavaRDD[String](rdd.rdd) {
    private val tRDD: TransformableRDD = new TransformableRDD(rdd.rdd)

    def deduplicate: JavaTransformableRDD = new JavaTransformableRDD(tRDD.deduplicate().toJavaRDD(), fileType)

    def impute(columnIndex: Int, imputationStrategy: ImputationStrategy): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.impute(columnIndex, imputationStrategy).toJavaRDD(), fileType)
    }

    def smooth(columnIndex: Int, smoothingMethod: SmoothingMethod): JavaRDD[java.lang.Double] = {
        tRDD.smooth(columnIndex, smoothingMethod).asInstanceOf[RDD[java.lang.Double]].toJavaRDD()
    }

    def clusters(columnIndex: Int, clusteringAlgorithm: ClusteringAlgorithm): JavaClusters = {
        new JavaClusters(tRDD.clusters(columnIndex, clusteringAlgorithm))
    }
}
