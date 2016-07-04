package org.apache.datacommons.prepbuddy.rdds

import java.lang.Double._
import java.security.MessageDigest

import org.apache.datacommons.prepbuddy.clusterers.TextFacets
import org.apache.datacommons.prepbuddy.imputations.ImputationStrategy
import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.datacommons.prepbuddy.utils.RowRecord
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable.Buffer

class TransformableRDD(parent: RDD[String], fileType: FileType = CSV) extends RDD[String](parent) {

    def removeRows(predicate: (RowRecord) => Boolean): TransformableRDD = {
        val filterFunction = (record: String) => {
            val rowRecord = new RowRecord(fileType.parse(record))
            !predicate(rowRecord)
        }
        val filteredRDD = this.filter(filterFunction)
        new TransformableRDD(filteredRDD, this.fileType)
    }

    def impute(columnIndex: Int, strategy: ImputationStrategy): TransformableRDD = {
        strategy.prepareSubstitute(this, columnIndex)
        val transformed: RDD[String] = map((record) => {
            val columns: Array[String] = fileType.parse(record)
            val value: String = columns(columnIndex)
            var replacementValue: String = value
            if (value.equals(null) || value.trim.isEmpty) {
                replacementValue = strategy.handleMissingData(new RowRecord(columns))
            }

            columns(columnIndex) = replacementValue
            fileType.join(columns)
        })

        new TransformableRDD(transformed, fileType)
    }

    def dropColumn(columnIndex: Int): TransformableRDD = {
        val transformed: RDD[String] = map((record: String) => {
            val recordInBuffer: Buffer[String] = fileType.parse(record).toBuffer
            recordInBuffer.remove(columnIndex)
            fileType.join(recordInBuffer.toArray)
        })
        new TransformableRDD(transformed, fileType)
    }

    private def generateFingerprintedRDD(primaryKeyColumns: List[Int]): RDD[(Long, String)] = {
        map(record => {
            val primaryKeyValues: Array[String] = extractPrimaryKeys(fileType.parse(record), primaryKeyColumns)
            val fingerprint = generateFingerprint(primaryKeyValues)
            (fingerprint, record)
        })
    }

    private def extractPrimaryKeys(columnValues: Array[String], primaryKeyIndexes: List[Int]): Array[String] = {
        if (primaryKeyIndexes.isEmpty)
            return columnValues

        var primaryKeyValues: Array[String] = Array()
        for (columnIndex <- primaryKeyIndexes)
            primaryKeyValues = primaryKeyValues.:+(columnValues(columnIndex))

        primaryKeyValues
    }

    def deduplicate(primaryKeyColumns: List[Int]): TransformableRDD = {
        val fingerprintedRDD: RDD[(Long, String)] = generateFingerprintedRDD(primaryKeyColumns)
        val reducedRDD: RDD[(Long, String)] = fingerprintedRDD.reduceByKey((accumulator, record) => record)
        new TransformableRDD(reducedRDD.values, fileType)
    }

    def deduplicate(): TransformableRDD = {
        deduplicate(List.empty)
    }

    def duplicates(primaryKeyColumns: List[Int]): TransformableRDD = {
        val fingerprintedRecord: RDD[(Long, String)] = generateFingerprintedRDD(primaryKeyColumns)

        val initialValue: List[String] = List[String]()
        val mergeValues: (List[String], String) => List[String] = (accumulator, value) => accumulator.::(value)
        val mergeCombiners: (List[String], List[String]) => List[String] = (agg1, agg2) => agg1 ::: agg2

        val recordsGroupedByKey: RDD[(Long, List[String])] = fingerprintedRecord.aggregateByKey(initialValue)(
            mergeValues,
            mergeCombiners
        )
        val duplicateRecords: RDD[String] = recordsGroupedByKey.filter(record => record._2.size != 1).flatMap(records => records._2)
        new TransformableRDD(duplicateRecords, fileType).deduplicate()
    }

    def listFacets(columnIndex: Int): TextFacets = {
        val columnValuePair: RDD[(String, Int)] = map((record) => {
            val columns: Array[String] = fileType.parse(record)
            (columns(columnIndex), 1)
        })
        val facets: RDD[(String, Int)] = columnValuePair.reduceByKey((accumulator, record) => {
            accumulator + record
        })
        new TextFacets(facets)
    }

    private def generateFingerprint(columns: Array[String]): Long = {
        val concatenatedString: String = columns.mkString("")
        val algorithm: MessageDigest = MessageDigest.getInstance("MD5")
        algorithm.update(concatenatedString.getBytes, 0, concatenatedString.length)
        BigInt(algorithm.digest()).longValue()
    }

    def toDoubleRDD(columnIndex: Int): RDD[Double] = {
        val filtered: RDD[String] = this.filter((record: String) => {
            val rowRecord: Array[String] = fileType.parse(record)
            val value: String = rowRecord.apply(columnIndex)
            val numberMatcher: String = "[+-]?\\d+.?\\d+"
            !value.trim.isEmpty || value.matches(numberMatcher) || value == null
        })

        filtered.map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val value: String = recordAsArray(columnIndex)
            parseDouble(value)
        })
    }

    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[String] = {
        parent.compute(split, context)
    }

    override protected def getPartitions: Array[Partition] = parent.partitions
}

