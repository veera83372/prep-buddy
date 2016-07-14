package org.apache.datacommons.prepbuddy.rdds

import java.security.MessageDigest

import org.apache.commons.lang.math.NumberUtils
import org.apache.datacommons.prepbuddy.clusterers.{Cluster, ClusteringAlgorithm, Clusters, TextFacets}
import org.apache.datacommons.prepbuddy.exceptions.{ApplicationException, ErrorMessages}
import org.apache.datacommons.prepbuddy.imputations.ImputationStrategy
import org.apache.datacommons.prepbuddy.normalizers.NormalizationStrategy
import org.apache.datacommons.prepbuddy.smoothers.SmoothingMethod
import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.datacommons.prepbuddy.utils.{PivotTable, RowRecord}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

class TransformableRDD(parent: RDD[String], fileType: FileType = CSV) extends AbstractRDD(parent, fileType) {

    def smooth(columnIndex: Int, smoothingMethod: SmoothingMethod): RDD[Double] = {
        validateColumnIndex(columnIndex)
        if (!isNumericColumn(columnIndex)) {
            throw new ApplicationException(ErrorMessages.COLUMN_VALUES_ARE_NOT_NUMERIC)
        }
        val cleanRDD: TransformableRDD = removeRows((rowRecord) => isNotNumber(rowRecord.valueAt(columnIndex)))
        val columnDataset: RDD[String] = cleanRDD.select(columnIndex)
        smoothingMethod.smooth(columnDataset)
    }

    def replaceValues(cluster: Cluster, newValue: String, columnIndex: Int): TransformableRDD = {
        val mapped: RDD[String] = map((row) => {
            val recordAsArray: Array[String] = fileType.parse(row)
            val value: String = recordAsArray(columnIndex)
            if (cluster.containsValue(value)) recordAsArray(columnIndex) = newValue
            fileType.join(recordAsArray)
        })
        new TransformableRDD(mapped, fileType)
    }

    def mapByFlag(symbol: String, symbolColumnIndex: Int, mapFunction: (String) => String): TransformableRDD = {
        val mappedRDD: RDD[String] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val symbolColumn: String = recordAsArray(symbolColumnIndex)
            if (symbolColumn.equals(symbol)) mapFunction(record) else record
        })
        new TransformableRDD(mappedRDD, fileType)
    }

    def flag(symbol: String, markerPredicate: (RowRecord) => Boolean): TransformableRDD = {
        val flagged: RDD[String] = map((record) => {
            var newRow: String = fileType.appendDelimiter(record)
            val recordAsArray: Array[String] = fileType.parse(record)
            if (markerPredicate(new RowRecord(recordAsArray))) newRow += symbol
            newRow
        })
        new TransformableRDD(flagged, fileType)
    }


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
        val columnValuePair: RDD[(String, Int)] = map(record => (fileType.valueAt(record, columnIndex), 1))
        val facets: RDD[(String, Int)] = columnValuePair.reduceByKey(_ + _)
        new TextFacets(facets)
    }

    def listFacets(columnIndexes: List[Int]): TextFacets = {
        validateColumnIndex(columnIndexes)
        val columnValuePair: RDD[(String, Int)] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val joinedValue: String = columnIndexes.map(recordAsArray(_)).mkString("\n")
            (joinedValue, 1)
        })
        val facets: RDD[(String, Int)] = columnValuePair.reduceByKey(_ + _)
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
            val firstColumnValue: String = fileType.valueAt(row, firstColumn)
            val secondColumnValue: String = fileType.valueAt(row, secondColumn)
            firstColumnValue.toDouble * secondColumnValue.toDouble
        })
    }

    private def isNotNumber(value: String): Boolean = !NumberUtils.isNumber(value)

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
        independentColumnIndexes.foreach((index) => {
            val facets: TextFacets = listFacets(pivotalColumn :: index :: Nil)
            val tuples = facets.rdd.collect()
            tuples.foreach((tuple) => {
                val split: Array[String] = tuple._1.split("\n")
                table.addEntry(split(0), split(1), tuple._2)
            })
        })
        table
    }

    def splitByFieldLength(column: Int, fieldLengths: List[Int], retainColumn: Boolean = false): TransformableRDD = {
        validateColumnIndex(column)
        val transformed: RDD[String] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val splitValue: Array[String] = splitStringByLength(recordAsArray(column), fieldLengths)
            arrangeRecords(recordAsArray, column :: Nil, splitValue, retainColumn)
        })
        new TransformableRDD(transformed, fileType)
    }

    private def splitStringByLength(value: String, lengths: List[Int]): Array[String] = {
        var result = Array.empty[String]
        var startingIndex = 0
        lengths.foreach((length) => {
            val endingIndex: Int = startingIndex + length
            val splitValue: String = value.substring(startingIndex, endingIndex)
            result = result.:+(splitValue)
            startingIndex = startingIndex + length
        })
        result
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

    def splitByDelimiter(column: Int, delimiter: String, retainColumn: Boolean): TransformableRDD = {
        splitByDelimiter(column, delimiter, -1, retainColumn)
    }

    def splitByDelimiter(column: Int, delimiter: String): TransformableRDD = splitByDelimiter(column, delimiter, -1)

    def mergeColumns(columns: List[Int], separator: String = " ", retainColumns: Boolean = false): TransformableRDD = {
        validateColumnIndex(columns)
        val transformedRDD: RDD[String] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val mergedValue: String = columns.map(recordAsArray(_)).mkString(separator)
            arrangeRecords(recordAsArray, columns, Array(mergedValue), retainColumns)
        })
        new TransformableRDD(transformedRDD, fileType)
    }

    private def arrangeRecords(values: Array[String], cols: List[Int], result: Array[String], retainColumn: Boolean) = {
        var arrangedRecord = values
        if (!retainColumn) {
            arrangedRecord = removeColumns(values, cols)
        }
        fileType.join(arrangedRecord ++ result)
    }

    private def removeColumns(values: Array[String], columns: List[Int]): Array[String] = {
        values.view.zipWithIndex
            .filterNot { case (value, index) => columns.contains(index) }
            .map(_._1)
            .toArray
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
        val columnsToBeSelected: Array[Int] = columnIndexes.+:(columnIndex).toArray
        validateColumnIndex(columnsToBeSelected.toList)
        val selectedColumnValues: RDD[String] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val resultValues: Array[String] = columnsToBeSelected.map(recordAsArray(_))
            fileType.join(resultValues)
        })
        new TransformableRDD(selectedColumnValues, fileType)
    }

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

    def impute(column: Int, strategy: ImputationStrategy): TransformableRDD = impute(column, strategy, List.empty)

    def drop(columnIndex: Int, columnIndexes: Int*): TransformableRDD = {
        val columnsToBeDroped: Seq[Int] = columnIndexes.+:(columnIndex)
        validateColumnIndex(columnsToBeDroped.toList)
        val transformed: RDD[String] = map((record: String) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val resultRecord: Array[String] = recordAsArray.view.zipWithIndex
                .filterNot { case (datum, index) => columnsToBeDroped.contains(index) }
                .map(_._1).toArray
            fileType.join(resultRecord)
        })
        new TransformableRDD(transformed, fileType)
    }

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

    def duplicates(): TransformableRDD = duplicates(List.empty)

    def duplicatesAt(columnIndex: Int): TransformableRDD = {
        validateColumnIndex(columnIndex)
        val specifiedColumnValues: RDD[String] = map(fileType.valueAt(_, columnIndex))
        new TransformableRDD(specifiedColumnValues, fileType).duplicates()
    }

    def unique(columnIndex: Int): TransformableRDD = {
        validateColumnIndex(columnIndex)
        val specifiedColumnValues: RDD[String] = map(fileType.valueAt(_, columnIndex))
        new TransformableRDD(specifiedColumnValues, fileType).deduplicate()
    }

    def deduplicate(primaryKeyColumns: List[Int]): TransformableRDD = {
        validateColumnIndex(primaryKeyColumns)
        val fingerprintedRDD: RDD[(Long, String)] = generateFingerprintedRDD(primaryKeyColumns)
        val reducedRDD: RDD[(Long, String)] = fingerprintedRDD.reduceByKey((accumulator, record) => record)
        new TransformableRDD(reducedRDD.values, fileType)
    }

    def deduplicate(): TransformableRDD = deduplicate(List.empty)

    private def generateFingerprintedRDD(primaryKeyColumns: List[Int]): RDD[(Long, String)] = {
        map(record => {
            val recordAsArray: Array[String] = fileType.parse(record)
            var primaryKeyValues: Array[String] = recordAsArray
            if (primaryKeyColumns.nonEmpty) {
                primaryKeyValues = primaryKeyColumns.map(recordAsArray(_)).toArray
            }
            val fingerprint = generateFingerprint(primaryKeyValues)
            (fingerprint, record)
        })
    }

    private def generateFingerprint(columns: Array[String]): Long = {
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