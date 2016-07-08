package org.apache.datacommons.prepbuddy.rdds

import java.lang.Double._

import org.apache.commons.lang.math.NumberUtils
import org.apache.datacommons.prepbuddy.exceptions.{ApplicationException, ErrorMessages}
import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

abstract class AbstractRDD(parent: RDD[String], fileType: FileType = CSV) extends RDD[String](parent) {
    val DEFAULT_SAMPLE_SIZE: Int = 1000
    protected val sampleRecords = takeSample(withReplacement = false, num = DEFAULT_SAMPLE_SIZE)
    protected val columnLength = getNumberOfColumns

    private def getNumberOfColumns: Int = {
        val columnLengthAndCount: mutable.HashMap[Int, Int] = new mutable.HashMap[Int, Int]()
        sampleRecords.foreach((record) => {
            val columnCount: Int = fileType.parse(record).length
            if (columnLengthAndCount.contains(columnCount)) {
                val count: Int = columnLengthAndCount.apply(columnCount)
                columnLengthAndCount.put(columnCount, count + 1)
            }
            else {
                columnLengthAndCount.put(columnCount, 1)
            }
        })
        getHighestCountKey(columnLengthAndCount)
    }

    private def getHighestCountKey(lengthWithCount: mutable.HashMap[Int, Int]): Int = {
        lengthWithCount.reduce((first, second) => {
            if (first._2 > second._2) first else second
        })._1
    }

    protected def validateColumnIndex(columnIndex: Int): Unit = {
        validateColumnIndex(List(columnIndex))
    }

    protected def validateColumnIndex(columnIndexes: List[Int]): Unit = {
        for (index <- columnIndexes) {
            if (columnLength <= index) {
                throw new ApplicationException(ErrorMessages.COLUMN_INDEX_OUT_OF_BOUND)
            }
            else if (index < 0) {
                throw new ApplicationException(ErrorMessages.NEGATIVE_COLUMN_INDEX)
            }
        }
    }

    def toDoubleRDD(columnIndex: Int): RDD[Double] = {
        validateColumnIndex(columnIndex)
        val filtered: RDD[String] = this.filter(record => {
            val value: String = fileType.parse(record)(columnIndex)
            NumberUtils.isNumber(value) && (value != null && !value.trim.isEmpty)
        })
        filtered.map(record => parseDouble(fileType.parse(record)(columnIndex)))
    }
}
