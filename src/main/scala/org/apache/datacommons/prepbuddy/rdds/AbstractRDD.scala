package org.apache.datacommons.prepbuddy.rdds

import java.lang.Double._

import org.apache.commons.lang.math.NumberUtils
import org.apache.datacommons.prepbuddy.exceptions.{ApplicationException, ErrorMessages}
import org.apache.datacommons.prepbuddy.qualityanalyzers.{BaseDataType, DataType, NUMERIC, TypeAnalyzer}
import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.datacommons.prepbuddy.utils.RowRecord
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

abstract class AbstractRDD(parent: RDD[String], fileType: FileType = CSV) extends RDD[String](parent) {
    val DEFAULT_SAMPLE_SIZE: Int = 1000
    protected val sampleRecords = takeSample(withReplacement = false, num = DEFAULT_SAMPLE_SIZE).toList
    protected val columnLength = getNumberOfColumns

    def toRDD: RDD[String] = parent

    def select(columnIndexes: List[Int]): TransformableRDD = {
        validateColumnIndex(columnIndexes)
        val selectedColumnValues: RDD[String] = map((record) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val resultValues: RowRecord = rowRecord.select(columnIndexes)
            fileType.join(resultValues)
        })
        new TransformableRDD(selectedColumnValues, fileType)
    }

    def select(columnIndex: Int): RDD[String] = select(List(columnIndex)).toRDD

    private def isNumericColumn(columnIndex: Int): Boolean = {
        val records: Array[String] = select(columnIndex).takeSample(withReplacement = false, num = DEFAULT_SAMPLE_SIZE)
        val baseType: BaseDataType = new TypeAnalyzer(records.toList).getBaseType
        baseType.equals(NUMERIC)
    }

    private def getNumberOfColumns: Int = {
        val columnLengthWithOccurrence: Map[Int, Int] = sampleRecords.view
            .groupBy(fileType.parse(_).length)
            .mapValues(_.length)
        if (columnLengthWithOccurrence.isEmpty) 0 else columnLengthWithOccurrence.maxBy(_._2)._1
    }

    protected def validateColumnIndex(columnIndex: Int): Unit = validateColumnIndex(List(columnIndex))

    protected def validateColumnIndex(columnIndexes: List[Int]) {
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
        validateNumericColumn(columnIndex)
        val filtered: RDD[String] = filter(record => {
            val value: String = fileType.parse(record).select(columnIndex)
            NumberUtils.isNumber(value)
        })
        filtered.map(record => parseDouble(fileType.parse(record).select(columnIndex)))
    }

    protected def validateNumericColumn(columnIndex: Int): Unit = {
        if (!isNumericColumn(columnIndex)) {
            throw new ApplicationException(ErrorMessages.COLUMN_VALUES_ARE_NOT_NUMERIC)
        }
    }

    def sampleColumnValues(columnIndex: Int): List[String] = sampleRecords.map(fileType.parse(_).select(columnIndex))

    def inferType(columnIndex: Int): DataType = {
        validateColumnIndex(columnIndex)
        val columnSamples: List[String] = sampleColumnValues(columnIndex)
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(columnSamples)
        typeAnalyzer.getType
    }

    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[String] = {
        parent.compute(split, context)
    }

    override protected def getPartitions: Array[Partition] = parent.partitions
}
