package org.apache.datacommons.prepbuddy.rdds

import java.security.MessageDigest

import org.apache.commons.lang.math.NumberUtils
import org.apache.datacommons.prepbuddy.clusterers.{ClusteringAlgorithm, Clusters, TextFacets}
import org.apache.datacommons.prepbuddy.imputations.ImputationStrategy
import org.apache.datacommons.prepbuddy.normalizers.NormalizationStrategy
import org.apache.datacommons.prepbuddy.qualityanalyzers.{DataType, TypeAnalyzer}
import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.datacommons.prepbuddy.utils.{PivotTable, RowRecord}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable

class TransformableRDD(parent: RDD[String], fileType: FileType = CSV) extends AbstractRDD(parent, fileType) {

    def numberOfColumns(): Int = columnLength

    def clusters(columnIndex: Int, algorithm: ClusteringAlgorithm): Clusters = {
        validateColumnIndex(columnIndex)
        val textFacets: TextFacets = listFacets(columnIndex)
        val rdd: RDD[(String, Int)] = textFacets.rdd
        val tuples: Array[(String, Int)] = rdd.collect

        algorithm.getClusters(tuples)
    }

    def listFacets(columnIndex: Int): TextFacets = {
        validateColumnIndex(columnIndex)
        val columnValuePair: RDD[(String, Int)] = map((record) => {
            val columns: Array[String] = fileType.parse(record)
            (columns(columnIndex), 1)
        })
        val facets: RDD[(String, Int)] = columnValuePair.reduceByKey((accumulator, record) => {
            accumulator + record
        })
        new TextFacets(facets)
    }

    def multiplyColumns(firstColumn: Int, secondColumn: Int): RDD[Double] = {
        validateColumnIndex(firstColumn :: secondColumn :: Nil)
        val rddOfNumbers: TransformableRDD = removeRows((record) => {
            val firstColumnValue: String = record.valueAt(firstColumn)
            val secondColumnValue: String = record.valueAt(secondColumn)
            isNotNumber(secondColumnValue) || isNotNumber(firstColumnValue)
        })
        rddOfNumbers.map((row) => {
            val recordAsArray: Array[String] = fileType.parse(row)
            val firstColumnValue: String = recordAsArray(firstColumn)
            val secondColumnValue: String = recordAsArray(secondColumn)
            firstColumnValue.toDouble * secondColumnValue.toDouble
        })
    }

    private def isNotNumber(value: String): Boolean = {
        !NumberUtils.isNumber(value)
    }

    def removeRows(predicate: (RowRecord) => Boolean): TransformableRDD = {
        val filteredRDD = filter((record: String) => {
            val rowRecord = new RowRecord(fileType.parse(record))
            !predicate(rowRecord)
        })
        new TransformableRDD(filteredRDD, fileType)
    }

    def pivotByCount(pivotalColumn: Int, independentColumnIndexes: Seq[Int]): PivotTable[Integer] = {
        validateColumnIndex(independentColumnIndexes.+:(pivotalColumn).toList)
        val table: PivotTable[Integer] = new PivotTable[Integer](0)
        independentColumnIndexes.foreach((each) => {
            val facets: TextFacets = listFacets(Array(pivotalColumn, each))
            val tuples = facets.rdd.collect()
            tuples.foreach((tuple) => {
                val split: Array[String] = tuple._1.split("\n")
                table.addEntry(split(0), split(1), tuple._2)
            })
        })
        table
    }

    def listFacets(columnIndexes: Array[Int]): TextFacets = {
        validateColumnIndex(columnIndexes.toList)
        val columnValuePair: RDD[(String, Int)] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            var joinValue: String = ""
            columnIndexes.foreach(joinValue += recordAsArray(_) + "\n")
            (joinValue.trim, 1)
        })
        val facets: RDD[(String, Int)] = {
            columnValuePair.reduceByKey((accumulator, currentValue) => accumulator + currentValue)
        }
        new TextFacets(facets)
    }

    def splitByFieldLength(column: Int, fieldLengths: List[Int], retainColumn: Boolean = false): TransformableRDD = {
        validateColumnIndex(column)
        val transformed: RDD[String] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val splitValue: Array[String] = splitStringByLength(recordAsArray(column), fieldLengths)
            arrangeRecords(recordAsArray, List(column), splitValue, retainColumn)
        })
        new TransformableRDD(transformed, fileType)
    }

    private def splitStringByLength(value: String, lengths: List[Int]): Array[String] = {
        var result: Array[String] = Array.empty[String]
        var startingIndex: Int = 0
        lengths.foreach((length) => {
            val endingIndex: Int = startingIndex + length
            val splitValue: String = value.substring(startingIndex, endingIndex)
            result = result.:+(splitValue)
            startingIndex = startingIndex + length
        })
        result
    }

    def splitByDelimiter(column: Int, delimiter: String, retainColumn: Boolean): TransformableRDD = {
        validateColumnIndex(column)
        splitByDelimiter(column, delimiter, -1, retainColumn)
    }

    def splitByDelimiter(col: Int, delimiter: String, maxSplit: Int, retainCol: Boolean = false): TransformableRDD = {
        validateColumnIndex(col)
        val transformed: RDD[String] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val splitValue: Array[String] = recordAsArray(col).split(delimiter, maxSplit)
            arrangeRecords(recordAsArray, List(col), splitValue, retainCol)
        })
        new TransformableRDD(transformed, fileType)
    }

    def mergeColumns(columns: List[Int], separator: String = " ", retainColumns: Boolean = false): TransformableRDD = {
        validateColumnIndex(columns)
        val transformedRDD: RDD[String] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val mergedValue: String = mergeValues(recordAsArray, columns, separator)
            arrangeRecords(recordAsArray, columns, Array(mergedValue), retainColumns)
        })
        new TransformableRDD(transformedRDD, fileType)
    }

    private def arrangeRecords(values: Array[String], cols: List[Int], result: Array[String], retainColumn: Boolean) = {
        var arrangedRecord = values
        if (!retainColumn) {
            arrangedRecord = removeElements(values, cols)
        }
        fileType.join(arrangedRecord ++ result)
    }

    private def removeElements(values: Array[String], columns: List[Int]): Array[String] = {
        var result: mutable.Buffer[String] = mutable.Buffer.empty
        for (index <- values.indices) {
            if (!columns.contains(index)) {
                result += values(index)
            }
        }
        result.toArray
    }

    def splitByDelimiter(column: Int, delimiter: String): TransformableRDD = splitByDelimiter(column, delimiter, -1)

    private def mergeValues(values: Array[String], combineOrder: List[Int], separator: String): String = {
        var mergedValue = ""
        combineOrder.foreach(mergedValue += separator + values(_))
        mergedValue.substring(separator.length, mergedValue.length)
    }

    def normalize(columnIndex: Int, normalizer: NormalizationStrategy): TransformableRDD = {
        validateColumnIndex(columnIndex)
        normalizer.prepare(this, columnIndex)
        val rdd: RDD[String] = map((record) => {
            val columns: Array[String] = fileType.parse(record)
            val normalizedColumn = normalizer.normalize(columns(columnIndex))
            columns(columnIndex) = normalizedColumn
            fileType.join(columns)
        })
        new TransformableRDD(rdd, fileType)
    }

    def select(columnIndex: Int, columnIndexes: Int*): TransformableRDD = {
        val allColumns: Seq[Int] = columnIndexes.+:(columnIndex)
        validateColumnIndex(allColumns.toList)
        val columnsToBeSelected: Array[Int] = allColumns.toArray
        val selectedColumnValues: RDD[String] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val resultValues: Array[String] = columnsToBeSelected.map(recordAsArray(_))
            fileType.join(resultValues)
        })
        new TransformableRDD(selectedColumnValues, fileType)
    }

    def impute(column: Int, strategy: ImputationStrategy): TransformableRDD = impute(column, strategy, List.empty)

    def impute(columnIndex: Int, strategy: ImputationStrategy, missingHints: List[String]): TransformableRDD = {
        validateColumnIndex(columnIndex)
        strategy.prepareSubstitute(this, columnIndex)
        val transformed: RDD[String] = map((record) => {
            val columns: Array[String] = fileType.parse(record)
            val value: String = columns(columnIndex)
            var replacementValue: String = value
            if (value.equals(null) || value.trim.isEmpty || missingHints.contains(value)) {
                replacementValue = strategy.handleMissingData(new RowRecord(columns))
            }
            columns(columnIndex) = replacementValue
            fileType.join(columns)
        })

        new TransformableRDD(transformed, fileType)
    }

    def drop(columnIndex: Int, columnIndexes: Int*): TransformableRDD = {
        val allColumns: Seq[Int] = columnIndexes.+:(columnIndex)
        validateColumnIndex(allColumns.toList)
        val transformed: RDD[String] = map((record: String) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val resultRecord: Array[String] = recordAsArray.zipWithIndex
                .filter { case (datum, index) => !allColumns.contains(index) }
                .map(_._1)
            fileType.join(resultRecord)
        })
        new TransformableRDD(transformed, fileType)
    }

    def duplicatesAt(columnIndex: Int): TransformableRDD = {
        validateColumnIndex(columnIndex)
        val specifiedColumnValues: RDD[String] = map(fileType.parse(_)(columnIndex))
        new TransformableRDD(specifiedColumnValues, fileType).duplicates()
    }

    def duplicates(): TransformableRDD = duplicates(List.empty)

    def duplicates(primaryKeyColumns: List[Int]): TransformableRDD = {
        validateColumnIndex(primaryKeyColumns)
        val fingerprintedRecord: RDD[(Long, String)] = generateFingerprintedRDD(primaryKeyColumns)
        val recordsGroupedByKey: RDD[(Long, List[String])] = fingerprintedRecord.aggregateByKey(List.empty[String])(
            (accumulatorValues, currentValue) => accumulatorValues.::(currentValue),
            (aggregator1, aggregator2) => aggregator1 ::: aggregator2
        )
        val duplicateRecords: RDD[String] = recordsGroupedByKey.filter(_._2.size != 1).flatMap(_._2)
        new TransformableRDD(duplicateRecords, fileType).deduplicate()
    }

    def unique(columnIndex: Int): TransformableRDD = {
        validateColumnIndex(columnIndex)
        val specifiedColumnValues: RDD[String] = map(fileType.parse(_)(columnIndex))
        new TransformableRDD(specifiedColumnValues, fileType).deduplicate()
    }

    def deduplicate(): TransformableRDD = deduplicate(List.empty)

    def deduplicate(primaryKeyColumns: List[Int]): TransformableRDD = {
        validateColumnIndex(primaryKeyColumns)
        val fingerprintedRDD: RDD[(Long, String)] = generateFingerprintedRDD(primaryKeyColumns)
        val reducedRDD: RDD[(Long, String)] = fingerprintedRDD.reduceByKey((accumulator, record) => record)
        new TransformableRDD(reducedRDD.values, fileType)
    }

    private def generateFingerprintedRDD(primaryKeyColumns: List[Int]): RDD[(Long, String)] = {
        map(record => {
            val columns: Array[String] = fileType.parse(record)
            val primaryKeyValues: Array[String] = extractPrimaryKeys(columns, primaryKeyColumns)
            val fingerprint = generateFingerprint(primaryKeyValues)
            (fingerprint, record)
        })
    }

    private def extractPrimaryKeys(columnValues: Array[String], primaryKeyIndexes: List[Int]): Array[String] = {
        if (primaryKeyIndexes.isEmpty) {
            return columnValues
        }
        var primaryKeyValues: Array[String] = Array.empty
        primaryKeyIndexes.foreach(index => primaryKeyValues = primaryKeyValues.:+(columnValues(index)))
        primaryKeyValues
    }

    private def generateFingerprint(columns: Array[String]): Long = {
        val concatenatedString: String = columns.mkString("")
        val algorithm: MessageDigest = MessageDigest.getInstance("MD5")
        algorithm.update(concatenatedString.getBytes, 0, concatenatedString.length)
        BigInt(algorithm.digest()).longValue()
    }

    def inferType(columnIndex: Int): DataType = {
        validateColumnIndex(columnIndex)
        val columnSamples: List[String] = sampleColumnValues(columnIndex)
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(columnSamples)
        typeAnalyzer.getType
    }

    private def sampleColumnValues(columnIndex: Int): List[String] = {
        sampleRecords.map(fileType.parse(_)(columnIndex)).toList
    }

    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[String] = {
        parent.compute(split, context)
    }

    override protected def getPartitions: Array[Partition] = parent.partitions
}