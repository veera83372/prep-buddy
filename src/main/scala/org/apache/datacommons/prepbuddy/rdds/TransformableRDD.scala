package org.apache.datacommons.prepbuddy.rdds

import java.security.MessageDigest

import org.apache.datacommons.prepbuddy.cleansers.imputation.ImputationStrategy
import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.datacommons.prepbuddy.utils.RowRecord
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}


class TransformableRDD(parent: RDD[String], fileType: FileType = CSV) extends RDD[String](parent) {

  def impute(columnIndex: Int, strategy: ImputationStrategy): TransformableRDD = {
    strategy.prepareSubstitute(this, columnIndex)
    val transformed: RDD[String] = this.map((record) => {
      val columns: Array[String] = fileType.parseRecord(record)
      val value: String = columns(columnIndex)
      var replacementValue: String = null
      if (value == null || value.trim.isEmpty) {
        replacementValue = strategy.handleMissingData(new RowRecord(columns))
      }

      columns(columnIndex) = replacementValue
      fileType.join(columns)
    })

    new TransformableRDD(transformed, fileType)
  }


  def deduplicate(): TransformableRDD = {

    val mappedRDD: RDD[(Long, String)] = this.map((record) => {
      val columns: Array[String] = this.fileType.parseRecord(record)
      val fingerprint = generateFingerPrint(columns)
      (fingerprint, record)
    })

    val uniqueValuesRDD: RDD[(Long, String)] = mappedRDD.reduceByKey((accumulator, record) => {
      record
    })
    new TransformableRDD(uniqueValuesRDD.values, fileType)
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

