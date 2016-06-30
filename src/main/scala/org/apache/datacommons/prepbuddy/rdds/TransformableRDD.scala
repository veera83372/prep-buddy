package org.apache.datacommons.prepbuddy.rdds

import java.security.MessageDigest

import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable.Buffer

class TransformableRDD(parent: RDD[String], fileType: FileType = CSV) extends RDD[String](parent) {

    def dropColumn(columnIndex: Int): TransformableRDD = {
        val transformed: RDD[String] = map((record: String) => {
            val recordInBuffer: Buffer[String] = fileType.parseRecord(record).toBuffer
            recordInBuffer.remove(columnIndex)
            fileType.join(recordInBuffer.toArray)
        })
        new TransformableRDD(transformed, fileType)
    }


    def deduplicate(): TransformableRDD = {
        val fingerprintedRecords: RDD[(Long, String)] = map((record) => {
            val columns: Array[String] = fileType.parseRecord(record)
            val fingerprint = generateFingerPrint(columns)
            (fingerprint, record)
        })

        new TransformableRDD(getUniqueValues(fingerprintedRecords), fileType)
    }

    def deduplicate(columnIndexes: List[Int]): TransformableRDD = {
        val fingerprintedRDD: RDD[(Long, String)] = map((record) => {
            val columnsAsArray: Array[String] = fileType.parseRecord(record)
            val primaryKeys: Array[String] = getPrimaryKeyValues(columnIndexes, columnsAsArray)
            val fingerprint = generateFingerPrint(primaryKeys)
            (fingerprint, record)
        })

        new TransformableRDD(getUniqueValues(fingerprintedRDD), fileType)
    }

    private def getUniqueValues(fingerprintedRDD: RDD[(Long, String)]): RDD[String] = {
        val reducedRDD: RDD[(Long, String)] = fingerprintedRDD.reduceByKey((accumulator, record) => {
            record
        })
        reducedRDD.values
    }

    private def getPrimaryKeyValues(columnIndexes: List[Int], columnValues: Array[String]): Array[String] = {
        var primaryKeys: List[String] = List()
        for (columnIndex <- columnIndexes) {
            primaryKeys = primaryKeys.:+(columnValues(columnIndex))
        }
        primaryKeys.toArray
    }

    private def generateFingerPrint(columns: Array[String]): Long = {
        val concatenatedString: String = columns.mkString("")
        val algorithm: MessageDigest = MessageDigest.getInstance("MD5")
        algorithm.update(concatenatedString.getBytes, 0, concatenatedString.length)
        BigInt(algorithm.digest()).longValue()
    }


    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[String] = {
        parent.compute(split, context)
    }

    override protected def getPartitions: Array[Partition] = parent.partitions
}

