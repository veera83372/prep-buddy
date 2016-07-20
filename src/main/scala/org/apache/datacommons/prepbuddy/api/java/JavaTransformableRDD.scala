package org.apache.datacommons.prepbuddy.api.java

import java.util

import org.apache.datacommons.prepbuddy.clusterers.{ClusteringAlgorithm, TextFacets}
import org.apache.datacommons.prepbuddy.imputations.ImputationStrategy
import org.apache.datacommons.prepbuddy.normalizers.NormalizationStrategy
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.smoothers.SmoothingMethod
import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.datacommons.prepbuddy.utils.PivotTable
import org.apache.spark.api.java.function.Function
import org.apache.spark.api.java.{JavaDoubleRDD, JavaRDD}

import scala.collection.JavaConverters._

class JavaTransformableRDD(rdd: JavaRDD[String], fileType: FileType) extends JavaRDD[String](rdd.rdd) {

    private val tRDD: TransformableRDD = new TransformableRDD(rdd.rdd, fileType)

    def this(rdd: JavaRDD[String]) {
        this(rdd, CSV)
    }

    def removeRows(rowPurger: RowPurger): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.removeRows(rowPurger.evaluate).toJavaRDD(), fileType)
    }

    def deduplicate(primaryKeyColumns: util.List[Integer]): JavaTransformableRDD = {
        val scalaList: List[Int] = asScalaIntList(primaryKeyColumns.asScala.toList)
        new JavaTransformableRDD(tRDD.deduplicate(scalaList).toJavaRDD(), fileType)
    }

    def deduplicate: JavaTransformableRDD = new JavaTransformableRDD(tRDD.deduplicate().toJavaRDD(), fileType)

    def duplicates(primaryKeyColumns: util.List[Integer]): JavaTransformableRDD = {
        val scalaList: List[Int] = asScalaIntList(primaryKeyColumns.asScala.toList)
        new JavaTransformableRDD(tRDD.duplicates(scalaList).toJavaRDD(), fileType)
    }

    def duplicates: JavaTransformableRDD = new JavaTransformableRDD(tRDD.duplicates().toJavaRDD(), fileType)

    def impute(columnIndex: Int, strategy: ImputationStrategy, missingHint: util.List[String]): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.impute(columnIndex, strategy, missingHint.asScala.toList), fileType)
    }

    def impute(columnIndex: Int, imputationStrategy: ImputationStrategy): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.impute(columnIndex, imputationStrategy).toJavaRDD(), fileType)
    }

    def smooth(columnIndex: Int, smoothingMethod: SmoothingMethod): JavaDoubleRDD = {
        new JavaDoubleRDD(tRDD.smooth(columnIndex, smoothingMethod))
    }

    def clusters(columnIndex: Int, clusteringAlgorithm: ClusteringAlgorithm): JavaClusters = {
        new JavaClusters(tRDD.clusters(columnIndex, clusteringAlgorithm))
    }

    def listFacets(columnIndex: Int): TextFacets = tRDD.listFacets(columnIndex)

    def listFacets(columnIndexes: util.List[Integer]): TextFacets = {
        tRDD.listFacets(asScalaIntList(columnIndexes.asScala.toList))
    }

    private def asScalaIntList(ls: List[Integer]): List[Int] = ls.map(x => x: Int)

    def normalize(columnIndex: Int, normalizationStrategy: NormalizationStrategy): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.normalize(columnIndex, normalizationStrategy).toJavaRDD(), fileType)
    }

    @annotation.varargs
    def select(columnIndex: Int, columnIndexes: Int*): JavaRDD[String] = {
        tRDD.select(columnIndex, columnIndexes: _*).toJavaRDD()
    }

    def numberOfColumns: Int = tRDD.numberOfColumns()

    def pivotByCount(pivotalColumn: Int, independentColumnIndex: util.List[Integer]): PivotTable[Integer] = {
        tRDD.pivotByCount(pivotalColumn, asScalaIntList(independentColumnIndex.asScala.toList))
    }

    def mergeColumns(columnIndexes: util.List[Integer]): JavaTransformableRDD = {
        mergeColumns(columnIndexes = columnIndexes, separator = " ", retainColumn = false)
    }

    def mergeColumns(columnIndexes: util.List[Integer], separator: String, retainColumn: Boolean = false):
    JavaTransformableRDD = {
        val toScalaList: List[Int] = asScalaIntList(columnIndexes.asScala.toList)
        val mergedRDD: JavaRDD[String] = tRDD.mergeColumns(toScalaList, separator, retainColumn).toJavaRDD()
        new JavaTransformableRDD(mergedRDD, fileType)
    }

    def splitByFieldLength(columnIndex: Int, fieldLengths: util.List[Integer], retainColumn: Boolean):
    JavaTransformableRDD = {
        val toScalaList: List[Int] = asScalaIntList(fieldLengths.asScala.toList)
        val splitRDD: JavaRDD[String] = tRDD.splitByFieldLength(columnIndex, toScalaList, retainColumn).toJavaRDD()
        new JavaTransformableRDD(splitRDD, fileType)
    }

    def splitByDelimiter(columnIndex: Int, delimiter: String, retainColumn: Boolean): JavaTransformableRDD = {
        val rdd: JavaRDD[String] = tRDD.splitByDelimiter(columnIndex, delimiter, retainColumn).toJavaRDD()
        new JavaTransformableRDD(rdd, fileType)
    }

    def flag(symbol: String, markerPredicate: MarkerPredicate): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.flag(symbol, markerPredicate.evaluate).toJavaRDD(), fileType)
    }

    def mapByFlag(symbol: String, columnIndex: Int, function: Function[String, String]): JavaTransformableRDD = {
        val mappedRDD: JavaRDD[String] = tRDD.mapByFlag(symbol, columnIndex, function.call).toJavaRDD()
        new JavaTransformableRDD(mappedRDD, fileType)
    }

    def drop(columnIndex: Int): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.drop(columnIndex).toJavaRDD(), fileType)
    }

    def duplicatesAt(columnIndex: Int): JavaRDD[String] = tRDD.duplicatesAt(columnIndex).toJavaRDD()

    def addColumnsFrom(other: JavaTransformableRDD): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.addColumnsFrom(other.tRDD).toJavaRDD(), fileType)
    }

    def replaceValues(cluster: JavaCluster, newValue: String, columnIndex: Int): JavaTransformableRDD = {
        val replacedRDD: JavaRDD[String] = tRDD.replaceValues(cluster.scalaCluster, newValue, columnIndex).toJavaRDD()
        new JavaTransformableRDD(replacedRDD, fileType)
    }

    def unique(columnIndex: Int): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.unique(columnIndex).toJavaRDD(), fileType)
    }
}
