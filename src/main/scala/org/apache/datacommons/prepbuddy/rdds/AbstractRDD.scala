package org.apache.datacommons.prepbuddy.rdds

import java.lang.Double._

import org.apache.commons.lang.math.NumberUtils
import org.apache.datacommons.prepbuddy.exceptions.{ApplicationException, ErrorMessages}
import org.apache.datacommons.prepbuddy.qualityanalyzers.{BaseDataType, DataType, NUMERIC, TypeAnalyzer}
import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.spark.rdd.RDD

abstract class AbstractRDD(parent: RDD[String], fileType: FileType = CSV) extends RDD[String](parent) {
    val DEFAULT_SAMPLE_SIZE: Int = 1000
    protected val sampleRecords = takeSample(withReplacement = false, num = DEFAULT_SAMPLE_SIZE)
    protected val columnLength = getNumberOfColumns

    def select(columnIndex: Int, columnIndexes: Int*): TransformableRDD = {
        val columnsToBeSelected: Array[Int] = columnIndexes.+:(columnIndex).toArray
        validateColumnIndex(columnsToBeSelected.toList)
        val selectedColumnValues: RDD[String] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val resultValues: Array[String] = columnsToBeSelected.map(recordAsArray(_))
            fileType.join(resultValues)
        })
        new TransformableRDD(selectedColumnValues, fileType)
    }

    def isNumericColumn(columnIndex: Int): Boolean = {
        val records: Array[String] = select(columnIndex).takeSample(withReplacement = false, num = DEFAULT_SAMPLE_SIZE)
        val baseType: BaseDataType = new TypeAnalyzer(records.toList).getBaseType
        baseType.equals(NUMERIC)
    }

    private def getNumberOfColumns: Int = {
        val columnLengthWithOccurrence: Map[Int, Int] = sampleRecords
            .groupBy(fileType.parse(_).length)
            .mapValues(_.length)
        columnLengthWithOccurrence.maxBy(_._2)._1
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
            val value: String = fileType.valueAt(record, columnIndex)
            NumberUtils.isNumber(value)
        })
        filtered.map(record => parseDouble(fileType.valueAt(record, columnIndex)))
    }

    protected def validateNumericColumn(columnIndex: Int): Unit = {
        if (!isNumericColumn(columnIndex)) {
            throw new ApplicationException(ErrorMessages.COLUMN_VALUES_ARE_NOT_NUMERIC)
        }
    }

    def sampleColumnValues(columnIndex: Int): List[String] = sampleRecords.map(fileType.valueAt(_, columnIndex)).toList

    def inferType(columnIndex: Int): DataType = {
        validateColumnIndex(columnIndex)
        val columnSamples: List[String] = sampleColumnValues(columnIndex)
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(columnSamples)
        typeAnalyzer.getType
    }
}
