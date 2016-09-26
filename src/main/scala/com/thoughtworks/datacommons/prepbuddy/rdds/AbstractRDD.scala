package com.thoughtworks.datacommons.prepbuddy.rdds

import java.lang.Double._

import com.thoughtworks.datacommons.prepbuddy.exceptions.{ApplicationException, ErrorMessages}
import com.thoughtworks.datacommons.prepbuddy.qualityanalyzers.{BaseDataType, DataType, NUMERIC, TypeAnalyzer}
import com.thoughtworks.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.commons.lang.math.NumberUtils
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

abstract class AbstractRDD(parent: RDD[String], fileType: FileType = CSV) extends RDD[String](parent) {
    private val DEFAULT_SAMPLE_SIZE: Int = 1000
    protected val sampleRecords = takeSample(withReplacement = false, num = DEFAULT_SAMPLE_SIZE).toList
    protected val columnLength = getNumberOfColumns

    /**
      * Returns RDD
      *
      * @return RDD[String]
      */
    def toRDD: RDD[String] = parent

    /**
      * Returns a double RDD of given column index
      *
      * @param columnIndex Column index
      * @return RDD[Double]
      */
    def toDoubleRDD(columnIndex: Int): RDD[Double] = {
        validateColumnIndex(columnIndex)
        validateNumericColumn(columnIndex)
        val filtered: RDD[String] = filter(record => {
            val value: String = fileType.parse(record)(columnIndex)
            NumberUtils.isNumber(value)
        })
        filtered.map(record => parseDouble(fileType.parse(record)(columnIndex)))
    }

    protected def validateNumericColumn(columnIndex: Int): Unit = {
        if (!isNumericColumn(columnIndex)) {
            throw new ApplicationException(ErrorMessages.COLUMN_VALUES_ARE_NOT_NUMERIC)
        }
    }

    private def isNumericColumn(columnIndex: Int): Boolean = {
        val records: Array[String] = select(columnIndex).takeSample(withReplacement = false, num = DEFAULT_SAMPLE_SIZE)
        val baseType: BaseDataType = new TypeAnalyzer(records.toList).getBaseType
        baseType.equals(NUMERIC)
    }

    /**
      * Returns a RDD of given column
      *
      * @param columnIndex Column index
      * @return RDD[String]
      */
    def select(columnIndex: Int): RDD[String] = map(fileType.parse(_)(columnIndex))

    protected def validateColumnIndex(columnIndex: Int, otherIndexes: Int*) {
        val allIndexes: List[Int] = columnIndex :: otherIndexes.toList
        validateColumnIndex(allIndexes)
    }

    protected def validateColumnIndex(columnIndexes: List[Int]) {
        for (index <- columnIndexes) {
            if (index < 0 || columnLength <= index) throw new ApplicationException(ErrorMessages.COLUMN_NOT_FOUND)
        }
    }

    /**
      * Returns inferred DataType of @columnIndex
      *
      * @param columnIndex Column Index on which type will be infered
      * @return DataType
      */
    def inferType(columnIndex: Int): DataType = {
        validateColumnIndex(columnIndex)
        val columnSamples: List[String] = sampleColumnValues(columnIndex)
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(columnSamples)
        typeAnalyzer.getType
    }

    /**
      * Returns a List of some elements of @columnIndex
      *
      * @param columnIndex
      * @return List[String]
      */
    def sampleColumnValues(columnIndex: Int): List[String] = sampleRecords.map(fileType.parse(_)(columnIndex))

    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[String] = {
        parent.compute(split, context)
    }

    override protected def getPartitions: Array[Partition] = parent.partitions

    private def getNumberOfColumns: Int = {
        val columnLengthWithOccurrence: Map[Int, Int] = sampleRecords.view
            .groupBy(fileType.parse(_).length)
            .mapValues(_.length)
        if (columnLengthWithOccurrence.isEmpty) 0 else columnLengthWithOccurrence.maxBy(_._2)._1
    }
}
