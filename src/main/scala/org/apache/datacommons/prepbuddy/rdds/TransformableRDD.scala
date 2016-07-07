package org.apache.datacommons.prepbuddy.rdds

import java.lang.Double._
import java.security.MessageDigest

import org.apache.commons.lang.math.NumberUtils
import org.apache.datacommons.prepbuddy.clusterers.{ClusteringAlgorithm, Clusters, TextFacets}
import org.apache.datacommons.prepbuddy.imputations.ImputationStrategy
import org.apache.datacommons.prepbuddy.normalizers.NormalizationStrategy
import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.datacommons.prepbuddy.utils.{PivotTable, RowRecord}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable

class TransformableRDD(parent: RDD[String], fileType: FileType = CSV) extends AbstractRDD(parent, fileType) {

    def numberOfColumns(): Int = columnLength

    def clusters(columnIndex: Int, algorithm: ClusteringAlgorithm): Clusters = {
        //validateColumnIndex(columnIndex)
        val textFacets: TextFacets = this.listFacets(columnIndex)
        val rdd: RDD[(String, Int)] = textFacets.rdd
        val tuples: Array[(String, Int)] = rdd.collect

        algorithm.getClusters(tuples)
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

    def multiplyColumns(firstColumn: Int, secondColumn: Int): RDD[Double] = {
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
        value.trim.isEmpty || !NumberUtils.isNumber(value)
    }

    def removeRows(predicate: (RowRecord) => Boolean): TransformableRDD = {
        val filterFunction = (record: String) => {
            val rowRecord = new RowRecord(fileType.parse(record))
            !predicate(rowRecord)
        }
        val filteredRDD = this.filter(filterFunction)
        new TransformableRDD(filteredRDD, this.fileType)
    }

    def pivotByCount(pivotalColumn: Int, independentColumnIndexes: Seq[Int]): PivotTable[Integer] = {
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
        val columnValuePair: RDD[(String, Int)] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            var joinValue: String = ""
            columnIndexes.foreach((index) => {
                joinValue += recordAsArray(index) + "\n"
            })
            (joinValue.trim, 1)
        })
        val facets: RDD[(String, Int)] = {
            columnValuePair.reduceByKey((accumulator, currentValue) => accumulator + currentValue)
        }
        new TextFacets(facets)
    }

    def splitByFieldLength(column: Int, fieldLengths: List[Int], retainColumn: Boolean = false): TransformableRDD = {
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
        splitByDelimiter(column, delimiter, -1, retainColumn)
    }

    def splitByDelimiter(column: Int, delimiter: String): TransformableRDD = {
        splitByDelimiter(column, delimiter, -1)
    }

    def splitByDelimiter(col: Int, delimiter: String, maxSplit: Int, retainCol: Boolean = false): TransformableRDD = {
        val transformed: RDD[String] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val splitValue: Array[String] = recordAsArray(col).split(delimiter, maxSplit)
            arrangeRecords(recordAsArray, List(col), splitValue, retainCol)
        })
        new TransformableRDD(transformed, fileType)
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

    def mergeColumns(columns: List[Int], separator: String = " ", retainColumns: Boolean = false): TransformableRDD = {
        val transformedRDD: RDD[String] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val mergedValue: String = mergeValues(recordAsArray, columns, separator)
            arrangeRecords(recordAsArray, columns, Array(mergedValue), retainColumns)
        })
        new TransformableRDD(transformedRDD, fileType)
    }

    private def mergeValues(values: Array[String], combineOrder: List[Int], separator: String): String = {
        var mergedValue = ""
        combineOrder.foreach((index) => mergedValue += separator + values(index))
        mergedValue.substring(separator.length, mergedValue.length)
    }

    def normalize(columnIndex: Int, normalizer: NormalizationStrategy): TransformableRDD = {
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
        val selectedColumnValues: RDD[String] = map((record) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val resultValues: Array[String] = columnsToBeSelected.map((index) => recordAsArray(index))
            fileType.join(resultValues)
        })
        new TransformableRDD(selectedColumnValues, fileType)
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

    def drop(columnIndex: Int, columnIndexes: Int*): TransformableRDD = {
        val columnsToDrop: Array[Int] = columnIndexes.+:(columnIndex).toArray
        val transformed: RDD[String] = map((record: String) => {
            val recordAsArray: Array[String] = fileType.parse(record)
            val resultRecord: Array[String] = recordAsArray.zipWithIndex
                .filter { case (datum, index) => !columnsToDrop.contains(index) }
                .map(_._1)
            fileType.join(resultRecord)
        })
        new TransformableRDD(transformed, fileType)
    }

    def duplicatesAt(columnIndex: Int): TransformableRDD = {
        val specifiedColumnValues: RDD[String] = map((record) => fileType.parse(record).apply(columnIndex))
        new TransformableRDD(specifiedColumnValues, fileType).duplicates()
    }

    def duplicates(): TransformableRDD = duplicates(List.empty)

    def duplicates(primaryKeyColumns: List[Int]): TransformableRDD = {
        val fingerprintedRecord: RDD[(Long, String)] = generateFingerprintedRDD(primaryKeyColumns)
        val recordsGroupedByKey: RDD[(Long, List[String])] = fingerprintedRecord.aggregateByKey(List.empty[String])(
            (accumulatorValues, currentValue) => accumulatorValues.::(currentValue),
            (aggregator1, aggregator2) => aggregator1 ::: aggregator2
        )
        val duplicateRecords: RDD[String] = recordsGroupedByKey
            .filter(record => record._2.size != 1)
            .flatMap(records => records._2)
        new TransformableRDD(duplicateRecords, fileType).deduplicate()
    }

    def unique(columnIndex: Int): TransformableRDD = {
        val specifiedColumnValues: RDD[String] = map((record) => fileType.parse(record)(columnIndex))
        new TransformableRDD(specifiedColumnValues, fileType).deduplicate()
    }

    def deduplicate(): TransformableRDD = deduplicate(List.empty)

    def deduplicate(primaryKeyColumns: List[Int]): TransformableRDD = {
        val fingerprintedRDD: RDD[(Long, String)] = generateFingerprintedRDD(primaryKeyColumns)
        val reducedRDD: RDD[(Long, String)] = {
            fingerprintedRDD.reduceByKey((accumulator, record) => record)
        }
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
        var primaryKeyValues: Array[String] = Array()
        primaryKeyIndexes.foreach((index) => primaryKeyValues = primaryKeyValues.:+(columnValues(index)))
        primaryKeyValues
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
            NumberUtils.isNumber(value) && (value != null && !value.trim.isEmpty)
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

