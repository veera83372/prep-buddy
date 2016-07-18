package org.apache.datacommons.prepbuddy.api.java

import java.util

import org.apache.datacommons.prepbuddy.clusterers.{ClusteringAlgorithm, TextFacets}
import org.apache.datacommons.prepbuddy.imputations.strategy
import org.apache.datacommons.prepbuddy.normalizers.NormalizationStrategy
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.smoothers.SmoothingMethod
import org.apache.datacommons.prepbuddy.types.FileType
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._


class JavaTransformableRDD(rdd: JavaRDD[String], fileType: FileType) extends JavaRDD[String](rdd.rdd) {
    private val tRDD: TransformableRDD = new TransformableRDD(rdd.rdd)

    def removeRows(rowPurger: RowPurger): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.removeRows(rowPurger.evaluate), fileType)
    }

    def deduplicate(primaryKeyColumns: util.List[Integer]): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.deduplicate(primaryKeyColumns.asScala.toList.asInstanceOf[List[Int]]), fileType)
    }
    def deduplicate: JavaTransformableRDD = new JavaTransformableRDD(tRDD.deduplicate().toJavaRDD(), fileType)

    def impute(columnIndex: Int, imputationStrategy: strategy, missingHints: util.List[String]): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.impute(columnIndex, imputationStrategy, missingHints.asScala.toList), fileType)
    }

    def impute(columnIndex: Int, imputationStrategy: strategy): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.impute(columnIndex, imputationStrategy).toJavaRDD(), fileType)
    }

    def smooth(columnIndex: Int, smoothingMethod: SmoothingMethod): JavaRDD[java.lang.Double] = {
        tRDD.smooth(columnIndex, smoothingMethod).asInstanceOf[RDD[java.lang.Double]].toJavaRDD()
    }

    def clusters(columnIndex: Int, clusteringAlgorithm: ClusteringAlgorithm): JavaClusters = {
        new JavaClusters(tRDD.clusters(columnIndex, clusteringAlgorithm))
    }

    def listFacets(columnIndex: Int): TextFacets = tRDD.listFacets(columnIndex)

    def normalize(columnIndex: Int, normalizationStrategy: NormalizationStrategy): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.normalize(columnIndex, normalizationStrategy).toJavaRDD(), fileType)
    }

    def select(columnIndex: Int): JavaRDD[String] = tRDD.select(columnIndex).toJavaRDD()
}
