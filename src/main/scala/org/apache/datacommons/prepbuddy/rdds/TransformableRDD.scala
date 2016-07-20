package org.apache.datacommons.prepbuddy.rdds

import org.apache.commons.lang.math.NumberUtils
import org.apache.datacommons.prepbuddy.clusterers.{Cluster, ClusteringAlgorithm, Clusters, TextFacets}
import org.apache.datacommons.prepbuddy.imputations.ImputationStrategy
import org.apache.datacommons.prepbuddy.normalizers.NormalizationStrategy
import org.apache.datacommons.prepbuddy.smoothers.SmoothingMethod
import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.datacommons.prepbuddy.utils.{PivotTable, RowRecord}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

class TransformableRDD(parent: RDD[String], fileType: FileType = CSV) extends AbstractRDD(parent, fileType) {

    private def getFileType: FileType = fileType

    def smooth(columnIndex: Int, smoothingMethod: SmoothingMethod): RDD[Double] = {
        validateColumnIndex(columnIndex)
        validateNumericColumn(columnIndex)
        val cleanRDD: TransformableRDD = removeRows((rowRecord) => isNotNumber(rowRecord.valueAt(columnIndex)))
        val columnDataset: RDD[String] = cleanRDD.select(columnIndex)
        smoothingMethod.smooth(columnDataset)
    }

    private def isNotNumber(value: String): Boolean = !NumberUtils.isNumber(value)

    def removeRows(predicate: (RowRecord) => Boolean): TransformableRDD = {
        val filteredRDD = filter((record: String) => {
            val rowRecord = fileType.parse(record)
            !predicate(rowRecord)
        })
        new TransformableRDD(filteredRDD, fileType)
    }

    def replaceValues(cluster: Cluster, newValue: String, columnIndex: Int): TransformableRDD = {
        val mapped: RDD[String] = map((row) => {
            var rowRecord: RowRecord = fileType.parse(row)
            val value: String = rowRecord.valueAt(columnIndex)
            if (cluster.containsValue(value)) rowRecord = rowRecord.replace(columnIndex, newValue)
            fileType.join(rowRecord)
        })
        new TransformableRDD(mapped, fileType)
    }

    def mapByFlag(symbol: String, symbolColumnIndex: Int, mapFunction: (String) => String): TransformableRDD = {
        val mappedRDD: RDD[String] = map((record) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val symbolColumn: String = rowRecord.valueAt(symbolColumnIndex)
            if (symbolColumn.equals(symbol)) mapFunction(record) else record
        })
        new TransformableRDD(mappedRDD, fileType)
    }

    def flag(symbol: String, markerPredicate: (RowRecord) => Boolean): TransformableRDD = {
        val flagged: RDD[String] = map((record) => {
            var newRow: String = fileType.appendDelimiter(record)
            val rowRecord: RowRecord = fileType.parse(record)
            if (markerPredicate(rowRecord)) newRow += symbol
            newRow
        })
        new TransformableRDD(flagged, fileType)
    }

    def numberOfColumns(): Int = columnLength

    def clusters(columnIndex: Int, clusteringAlgorithm: ClusteringAlgorithm): Clusters = {
        validateColumnIndex(columnIndex)
        val textFacets: TextFacets = listFacets(columnIndex)
        val rdd: RDD[(String, Int)] = textFacets.rdd
        val tuples: Array[(String, Int)] = rdd.collect

        clusteringAlgorithm.getClusters(tuples)
    }

    def listFacets(columnIndex: Int): TextFacets = {
        validateColumnIndex(columnIndex)
        val columnValuePair: RDD[(String, Int)] = map(record => (fileType.parse(record).valueAt(columnIndex), 1))
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
            val firstColumnValue: String = fileType.parse(row).valueAt(firstColumn)
            val secondColumnValue: String = fileType.parse(row).valueAt(secondColumn)
            firstColumnValue.toDouble * secondColumnValue.toDouble
        })
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

    def listFacets(columnIndexes: List[Int]): TextFacets = {
        validateColumnIndex(columnIndexes)
        val columnValuePair: RDD[(String, Int)] = map((record) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val joinedValue: String = columnIndexes.map(rowRecord.valueAt).mkString("\n")
            (joinedValue, 1)
        })
        val facets: RDD[(String, Int)] = columnValuePair.reduceByKey(_ + _)
        new TextFacets(facets)
    }

    def splitByFieldLength(column: Int, fieldLengths: List[Int], retainColumn: Boolean = false): TransformableRDD = {
        validateColumnIndex(column)
        val transformed: RDD[String] = map((record) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val splitValue: Array[String] = splitStringByLength(rowRecord.valueAt(column), fieldLengths)
            arrangeRecords(rowRecord, column :: Nil, splitValue, retainColumn)
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

    private def arrangeRecords(values: RowRecord, cols: List[Int], result: Array[String], retainColumn: Boolean) = {
        var arrangedRecord = values
        if (!retainColumn) {
            arrangedRecord = values.valuesNotAt(cols)
        }
        fileType.join(arrangedRecord.appendColumns(result))
    }

    def splitByDelimiter(column: Int, delimiter: String, retainColumn: Boolean): TransformableRDD = {
        splitByDelimiter(column, delimiter, -1, retainColumn)
    }

    def splitByDelimiter(column: Int, delimiter: String): TransformableRDD = splitByDelimiter(column, delimiter, -1)

    def splitByDelimiter(col: Int, delimiter: String, maxSplit: Int, retainCol: Boolean = false): TransformableRDD = {
        validateColumnIndex(col)
        val transformed: RDD[String] = map((record) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val splitValue: Array[String] = rowRecord.valueAt(col).split(delimiter, maxSplit)
            arrangeRecords(rowRecord, List(col), splitValue, retainCol)
        })
        new TransformableRDD(transformed, fileType)
    }

    def mergeColumns(columns: List[Int], separator: String = " ", retainColumns: Boolean = false): TransformableRDD = {
        validateColumnIndex(columns)
        val transformedRDD: RDD[String] = map((record) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val mergedValue: String = columns.map(rowRecord.valueAt).mkString(separator)
            arrangeRecords(rowRecord, columns, Array(mergedValue), retainColumns)
        })
        new TransformableRDD(transformedRDD, fileType)
    }

    def normalize(columnIndex: Int, normalizer: NormalizationStrategy): TransformableRDD = {
        validateColumnIndex(columnIndex)
        normalizer.prepare(this, columnIndex)
        val rdd: RDD[String] = map((record) => {
            var columns: RowRecord = fileType.parse(record)
            val normalizedColumn = normalizer.normalize(columns.valueAt(columnIndex))
            columns = columns.replace(columnIndex, normalizedColumn)
            fileType.join(columns)
        })
        new TransformableRDD(rdd, fileType)
    }

    def impute(column: Int, strategy: ImputationStrategy): TransformableRDD = impute(column, strategy, List.empty)

    def impute(columnIndex: Int, strategy: ImputationStrategy, missingHints: List[String]): TransformableRDD = {
        validateColumnIndex(columnIndex)
        strategy.prepareSubstitute(this, columnIndex)
        val transformed: RDD[String] = map((record) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val value: String = rowRecord.valueAt(columnIndex)
            var replacementValue: String = value
            if (value.equals(null) || value.trim.isEmpty || missingHints.contains(value)) {
                replacementValue = strategy.handleMissingData(rowRecord)
            }

            fileType.join(rowRecord.replace(columnIndex, replacementValue))
        })

        new TransformableRDD(transformed, fileType)
    }

    def drop(columnIndex: Int, columnIndexes: Int*): TransformableRDD = {
        val columnsToBeDroped: List[Int] = columnIndexes.+:(columnIndex).toList
        validateColumnIndex(columnsToBeDroped)
        val transformed: RDD[String] = map((record: String) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val resultRecord: RowRecord = rowRecord.valuesNotAt(columnsToBeDroped)
            fileType.join(resultRecord)
        })
        new TransformableRDD(transformed, fileType)
    }

    def duplicatesAt(columnIndex: Int): RDD[String] = {
        validateColumnIndex(columnIndex)
        val specifiedColumnValues: RDD[String] = map(fileType.parse(_).valueAt(columnIndex))
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

    def deduplicate(): TransformableRDD = deduplicate(List.empty)

    def deduplicate(primaryKeyColumns: List[Int]): TransformableRDD = {
        validateColumnIndex(primaryKeyColumns)
        val fingerprintedRDD: RDD[(Long, String)] = generateFingerprintedRDD(primaryKeyColumns)
        val reducedRDD: RDD[(Long, String)] = fingerprintedRDD.reduceByKey((accumulator, record) => record)
        new TransformableRDD(reducedRDD.values, fileType)
    }

    private def generateFingerprintedRDD(primaryKeyColumns: List[Int]): RDD[(Long, String)] = {
        map(record => {
            val rowRecord: RowRecord = fileType.parse(record)
            val fingerprint = rowRecord.fingerprintBy(primaryKeyColumns)
            (fingerprint, record)
        })
    }

    def unique(columnIndex: Int): TransformableRDD = {
        validateColumnIndex(columnIndex)
        val specifiedColumnValues: RDD[String] = map(fileType.parse(_).valueAt(columnIndex))
        new TransformableRDD(specifiedColumnValues, fileType).deduplicate()
    }

    def addColumnsFrom(otherRDD: TransformableRDD): TransformableRDD = {
        val otherRDDInCurrentFileFormat = {
            if (this.getFileType != otherRDD.getFileType) {
                otherRDD.map(record => {
                    val rowRecord: RowRecord = otherRDD.getFileType.parse(record)
                    fileType.join(rowRecord)
                })
            }
            else {
                otherRDD
            }
        }
        val zippedRecords: RDD[(String, String)] = zip(otherRDDInCurrentFileFormat)
        val recordsWithAddedColumns: RDD[String] = zippedRecords.map(row => fileType.appendDelimiter(row._1) + row._2)

        new TransformableRDD(recordsWithAddedColumns, fileType)
    }

    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[String] = {
        parent.compute(split, context)
    }

    override protected def getPartitions: Array[Partition] = parent.partitions
}
