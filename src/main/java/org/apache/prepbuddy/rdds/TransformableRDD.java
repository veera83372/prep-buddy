package org.apache.prepbuddy.rdds;

import org.apache.commons.lang.ArrayUtils;
import org.apache.prepbuddy.cleansers.dedupe.DuplicationHandler;
import org.apache.prepbuddy.cleansers.imputation.ImputationStrategy;
import org.apache.prepbuddy.cluster.Cluster;
import org.apache.prepbuddy.cluster.ClusteringAlgorithm;
import org.apache.prepbuddy.cluster.Clusters;
import org.apache.prepbuddy.cluster.TextFacets;
import org.apache.prepbuddy.exceptions.ApplicationException;
import org.apache.prepbuddy.exceptions.ErrorMessages;
import org.apache.prepbuddy.normalizers.NormalizationStrategy;
import org.apache.prepbuddy.qualityanalyzers.FileType;
import org.apache.prepbuddy.smoothers.SmoothingMethod;
import org.apache.prepbuddy.transformers.*;
import org.apache.prepbuddy.utils.PivotTable;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TransformableRDD extends AbstractRDD {

    public TransformableRDD(JavaRDD<String> rdd, FileType fileType) {
        super(rdd, fileType);
    }

    public TransformableRDD(JavaRDD<String> rdd) {
        super(rdd, FileType.CSV);
    }

    /**
     * Returns a new TransformableRDD containing only unique records by considering all the columns as the primary key.
     *
     * @return TransformableRDD A new TransformableRDD consisting only the unique records
     */
    public TransformableRDD deduplicate() {
        JavaRDD<String> transformed = DuplicationHandler.deduplicate(this);
        return new TransformableRDD(transformed, fileType);
    }

    /**
     * Returns a new TransformableRDD containing only unique records by considering the given the columns as the primary key.
     *
     * @param primaryColumnIndexes A list of integers specifying the columns that will be combined to create the primary key.
     * @return TransformableRDD A new TransformableRDD consisting records by eliminating all duplicates depending upon the primary key.
     */
    public TransformableRDD deduplicate(List<Integer> primaryColumnIndexes) {
        JavaRDD<String> transformed = DuplicationHandler.deduplicateByColumns(this, primaryColumnIndexes, fileType);
        return new TransformableRDD(transformed, fileType);
    }

    /**
     * Returns a new TransformableRDD containing unique duplicate records of this TransformableRDD by considering all the columns as primary key.
     *
     * @return TransformableRDD A new TransformableRDD consisting unique duplicate records.
     */
    public TransformableRDD getDuplicates() {
        JavaRDD<String> duplicates = DuplicationHandler.detectDuplicates(this);
        return new TransformableRDD(duplicates, fileType);
    }

    /**
     * Returns a new TransformableRDD containing unique duplicate records of this TransformableRDD by considering the given columns as primary key.
     *
     * @param primaryColumnIndexes A list of integers specifying the columns that will be combined to create the primary key
     * @return TransformableRDD A new TransformableRDD consisting unique duplicate records.
     */
    public TransformableRDD getDuplicates(List<Integer> primaryColumnIndexes) {
        JavaRDD<String> transformed = DuplicationHandler.detectDuplicatesByColumns(this, primaryColumnIndexes, fileType);
        return new TransformableRDD(transformed, fileType);
    }

    /**
     * Returns a new TransformableRDD containing only the duplicate elements from a particular column of the dataset
     *
     * @param columnIndex Column index
     * @return TransformableRDD
     */
    public TransformableRDD detectDuplicatesAt(int columnIndex) {
        JavaRDD<String> transformed = DuplicationHandler.duplicatesAt(this, columnIndex, fileType);
        return new TransformableRDD(transformed, fileType);
    }

    /**
     * Returns a new TransformableRDD containing only the elements that satisfy the matchInDictionary.
     *
     * @param predicate A matchInDictionary function, Removes the row when returns true.
     * @return TransformableRDD
     */
    public TransformableRDD removeRows(RowPurger.Predicate predicate) {
        JavaRDD<String> transformed = new RowPurger(predicate).apply(this, fileType);
        return new TransformableRDD(transformed, fileType);
    }

    /**
     * Returns a new TransformableRDD by replacing the @columnIndex values with value returned by @replacement
     *
     * @param columnIndex The column index where the replacement will occur
     * @param replacement A function which will return the replacement value
     * @return TransformableRDD
     */
    public TransformableRDD replace(final int columnIndex, final ReplacementFunction replacement) {
        validateColumnIndex(columnIndex);
        JavaRDD<String> transformed = this.map(new Function<String, String>() {

            @Override
            public String call(String record) throws Exception {
                String[] recordAsArray = fileType.parseRecord(record);
                recordAsArray[columnIndex] = replacement.replace(new RowRecord(recordAsArray));
                return fileType.join(recordAsArray);
            }
        });
        return new TransformableRDD(transformed, fileType);
    }

    /**
     * Returns a new TextFacet containing the cardinal values of @columnIndex
     *
     * @param columnIndex index of the column
     * @return TextFacets
     */
    public TextFacets listFacets(final int columnIndex) {
        validateColumnIndex(columnIndex);
        JavaPairRDD<String, Integer> columnValuePair = this.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String record) throws Exception {
                String[] columnValues = fileType.parseRecord(record);
                return new Tuple2<>(columnValues[columnIndex], 1);
            }
        });
        JavaPairRDD<String, Integer> facets = columnValuePair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer accumulator, Integer currentValue) throws Exception {
                return accumulator + currentValue;
            }
        });
        return new TextFacets(facets);
    }

    /**
     * Returns a new TextFacet containing the facets of @columnIndexes
     *
     * @param columnIndexes index of the column
     * @return TextFacets
     */
    public TextFacets listFacets(final int[] columnIndexes) {
        validateColumnIndex(columnIndexes);
        JavaPairRDD<String, Integer> columnValuePair = this.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String record) throws Exception {
                String[] columnValues = fileType.parseRecord(record);
                String joinValue = "";
                for (int columnIndex : columnIndexes) {
                    joinValue += "\n" + columnValues[columnIndex];
                }
                return new Tuple2<>(joinValue.trim(), 1);
            }
        });
        JavaPairRDD<String, Integer> facets = columnValuePair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer accumulator, Integer currentValue) throws Exception {
                return accumulator + currentValue;
            }
        });
        return new TextFacets(facets);
    }

    /**
     * Returns Clusters that has all cluster of text of @columnIndex according to @algorithm
     *
     * @param columnIndex Column Index
     * @param algorithm   Algorithm to be used to form clusters
     * @return Clusters
     */
    public Clusters clusters(int columnIndex, ClusteringAlgorithm algorithm) {
        validateColumnIndex(columnIndex);
        TextFacets textFacets = this.listFacets(columnIndex);
        JavaPairRDD<String, Integer> rdd = textFacets.rdd();
        List<Tuple2<String, Integer>> tuples = rdd.collect();

        return algorithm.getClusters(tuples);
    }

    /**
     * Returns a new TransformableRDD containing split columns using @splitPlan
     *
     * @param splitPlan Plan specifying how to split the column
     * @return TransformableRDD
     */
    public TransformableRDD splitColumn(final SplitPlan splitPlan) {
        JavaRDD<String> transformed = this.map(new Function<String, String>() {
            @Override
            public String call(String record) throws Exception {
                String[] recordAsArray = fileType.parseRecord(record);
                String[] transformedRow = splitPlan.splitColumn(recordAsArray);
                return fileType.join(transformedRow);
            }
        });
        return new TransformableRDD(transformed, fileType);
    }

    /**
     * Returns a new TransformableRDD containing the merged column using @mergePlan
     *
     * @param mergePlan A plan which describes how the marge operation will be done
     * @return TransformableRDD
     */
    public TransformableRDD mergeColumns(final MergePlan mergePlan) {
        JavaRDD<String> transformed = this.map(new Function<String, String>() {
            @Override
            public String call(String record) throws Exception {
                String[] recordAsArray = fileType.parseRecord(record);
                String[] transformedRow = mergePlan.apply(recordAsArray);
                return fileType.join(transformedRow);
            }
        });
        return new TransformableRDD(transformed, fileType);
    }

    /**
     * Returns a new TransformableRDD that contains records flagged by @symbol
     * based on the evaluation of @markerPredicate
     *
     * @param symbol          Symbol that will be used to flag
     * @param markerPredicate A matchInDictionary which will determine whether to flag a row or not
     * @return TransformableRDD
     */
    public TransformableRDD flag(final String symbol, final MarkerPredicate markerPredicate) {
        JavaRDD<String> transformed = this.map(new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                String newRow = fileType.appendDelimiter(row);
                if (markerPredicate.evaluate(new RowRecord(fileType.parseRecord(row))))
                    return newRow + symbol;
                return newRow;
            }
        });
        return new TransformableRDD(transformed, fileType);
    }

    /**
     * Returns a new TransformableRDD by applying the function on all rows marked as @flag
     *
     * @param flag              Symbol that has been used for flagging.
     * @param symbolColumnIndex Symbol column index
     * @param mapFunction       map function
     * @return TransformableRDD
     */
    public TransformableRDD mapByFlag(final String flag, final int symbolColumnIndex, final Function<String, String> mapFunction) {
        validateColumnIndex(symbolColumnIndex);
        JavaRDD<String> mappedRDD = this.map(new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                String[] records = fileType.parseRecord(row);
                String lastColumn = records[symbolColumnIndex];
                return lastColumn.equals(flag) ? mapFunction.call(row) : row;
            }
        });
        return new TransformableRDD(mappedRDD, fileType);
    }

    /**
     * Returns a new TransformableRDD by dropping the column at given index
     *
     * @param columnIndex The column that will be droped.
     * @return TransformableRDD
     */
    public TransformableRDD dropColumn(final int columnIndex) {
        validateColumnIndex(columnIndex);
        JavaRDD<String> mapped = this.map(new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                int newArrayIndex = 0;
                String[] recordAsArray = fileType.parseRecord(row);
                String[] newRecordArray = new String[recordAsArray.length - 1];
                for (int i = 0; i < recordAsArray.length; i++) {
                    String columnValue = recordAsArray[i];
                    if (i != columnIndex)
                        newRecordArray[newArrayIndex++] = columnValue;
                }
                return fileType.join(newRecordArray);
            }
        });
        return new TransformableRDD(mapped, fileType);
    }

    /**
     * Returns a new TransformableRDD by replacing the @cluster's text with specified @newValue
     *
     * @param cluster     Cluster of similar values to be replaced
     * @param newValue    Value that will be used to replace all the cluster value
     * @param columnIndex Column index
     * @return TransformableRDD
     */
    public TransformableRDD replaceValues(final Cluster cluster, final String newValue, final int columnIndex) {
        validateColumnIndex(columnIndex);
        JavaRDD<String> mapped = this.map(new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                String[] recordAsArray = fileType.parseRecord(row);
                String value = recordAsArray[columnIndex];
                if (cluster.containValue(value))
                    recordAsArray[columnIndex] = newValue;
                return fileType.join(recordAsArray);
            }
        });
        return new TransformableRDD(mapped, fileType);
    }

    /**
     * Returns a new TransformableRDD by imputing missing values of the @columnIndex using the @strategy
     *
     * @param columnIndex Column index
     * @param strategy    Imputation strategy
     * @return TransformableRDD
     */
    public TransformableRDD impute(final int columnIndex, final ImputationStrategy strategy) {
        validateColumnIndex(columnIndex);
        return impute(columnIndex, strategy, Collections.EMPTY_LIST);
    }

    /**
     * Returns a new TransformableRDD by imputing missing values of the @columnIndex using the @strategy
     *
     * @param columnIndex  Column Index
     * @param strategy     Imputation Strategy
     * @param missingHints List of Strings that may mean empty
     * @return TransformableRDD
     */
    public TransformableRDD impute(final int columnIndex, final ImputationStrategy strategy, final List<String> missingHints) {
        validateColumnIndex(columnIndex);
        strategy.prepareSubstitute(this, columnIndex);
        JavaRDD<String> transformed = this.map(new Function<String, String>() {

            @Override
            public String call(String record) throws Exception {
                String[] recordAsArray = fileType.parseRecord(record);
                String value = recordAsArray[columnIndex];
                String replacementValue = value;
                if (value == null || value.trim().isEmpty() || missingHints.contains(value)) {
                    replacementValue = strategy.handleMissingData(new RowRecord(recordAsArray));
                }
                recordAsArray[columnIndex] = replacementValue;
                return fileType.join(recordAsArray);
            }
        });
        return new TransformableRDD(transformed, fileType);
    }

    /**
     * Returns a JavaDoubleRDD which is a product of the values in @firstColumn and @secondColumn
     *
     * @param firstColumn   First Column Index
     * @param secondColumn Second Column Index
     * @return JavaDoubleRDD
     */
    public JavaDoubleRDD multiplyColumns(final int firstColumn, final int secondColumn) {
        validateColumnIndex(firstColumn, secondColumn);
        if (!isNumericColumn(firstColumn) || !isNumericColumn(secondColumn))
            throw new ApplicationException(ErrorMessages.COLUMN_VALUES_ARE_NOT_NUMERIC);

        return this.mapToDouble(new DoubleFunction<String>() {
            @Override
            public double call(String row) throws Exception {
                String[] recordAsArray = fileType.parseRecord(row);
                String columnValue = recordAsArray[firstColumn];
                String otherColumnValue = recordAsArray[secondColumn];
                if (columnValue.trim().isEmpty() || otherColumnValue.trim().isEmpty())
                    return 0;
                return Double.parseDouble(recordAsArray[firstColumn]) * Double.parseDouble(recordAsArray[secondColumn]);

            }
        });
    }

    /**
     * Returns a new TransformableRDD by normalizing values of the given column using different Normalizers
     *
     * @param columnIndex Column Index
     * @param normalizer  Normalization Strategy
     * @return TransformableRDD
     */
    public TransformableRDD normalize(final int columnIndex, final NormalizationStrategy normalizer) {
        validateColumnIndex(columnIndex);
        normalizer.prepare(this, columnIndex);

        JavaRDD<String> normalized = this.map(new Function<String, String>() {
            @Override
            public String call(String record) throws Exception {
                String[] columns = fileType.parseRecord(record);
                String normalized = normalizer.normalize(columns[columnIndex]);
                columns[columnIndex] = normalized;
                return fileType.join(columns);
            }
        });
        return new TransformableRDD(normalized);
    }

    /**
     * Returns a JavaRDD of given column
     *
     * @param columnIndex Column index
     * @return JavaRDD<String>
     */
    public JavaRDD<String> select(final int columnIndex) {
        validateColumnIndex(columnIndex);
        return map(new Function<String, String>() {
            @Override
            public String call(String record) throws Exception {
                return fileType.parseRecord(record)[columnIndex];
            }
        });
    }

    /**
     * Returns a new TransformableRDD containing values of @columnIndexes
     *
     * @param columnIndexes A number of integer values specifying the columns that will be used to create the new table
     * @return TransformableRDD
     */
    public TransformableRDD select(final int... columnIndexes) {
        validateColumnIndex(columnIndexes);
        JavaRDD<String> reducedRDD = map(new Function<String, String>() {
            @Override
            public String call(String record) throws Exception {
                String[] reducedRecord = new String[columnIndexes.length];
                String[] columns = fileType.parseRecord(record);
                for (int i = 0; i < columnIndexes.length; i++)
                    reducedRecord[i] = (columns[columnIndexes[i]]);
                return fileType.join(reducedRecord);
            }
        });
        return new TransformableRDD(reducedRDD, fileType);
    }

    /**
     * Generates a PivotTable by pivoting data in the pivotalColumn
     *
     * @param pivotalColumn            Pivotal Column
     * @param independentColumnIndexes Independent Column Indexes
     * @return PivotTable
     */
    public PivotTable pivotByCount(int pivotalColumn, int[] independentColumnIndexes) {
        validateColumnIndex(pivotalColumn);
        PivotTable<Integer> pivotTable = new PivotTable<>(0);
        for (int index : independentColumnIndexes) {
            TextFacets facets = listFacets(new int[]{pivotalColumn, index});
            List<Tuple2<String, Integer>> tuples = facets.rdd().collect();
            for (Tuple2<String, Integer> tuple : tuples) {
                String[] split = tuple._1().split("\n");
                pivotTable.addEntry(split[0], split[1], tuple._2());
            }
        }
        return pivotTable;
    }

    /**
     * Returns a new JavaRDD containing smoothed values of @columnIndex using @smoothingMethod
     *
     * @param columnIndex     Column Index
     * @param smoothingMethod Method that will be used for smoothing of the data
     * @return JavaRDD<Double>
     */
    public JavaRDD<Double> smooth(int columnIndex, SmoothingMethod smoothingMethod) {
        validateColumnIndex(columnIndex);
        JavaRDD<String> rdd = this.select(columnIndex);
        return smoothingMethod.smooth(rdd);
    }

    public TransformableRDD addColumnsFrom(final TransformableRDD other) {
        JavaPairRDD<String, String> thisAndOther = this.zip(other);
        final FileType otherFileType = other.fileType;
        JavaRDD<String> combinedRecords = thisAndOther.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> thisAndOtherRecord) throws Exception {
                String[] currentRecord = fileType.parseRecord(thisAndOtherRecord._1());
                String[] otherRecord = otherFileType.parseRecord(thisAndOtherRecord._2());
                return fileType.join((String[]) ArrayUtils.addAll(currentRecord, otherRecord));
            }
        });
        return new TransformableRDD(combinedRecords, fileType);
    }

    @Override
    public TransformableRDD map(Function function) {
        return new TransformableRDD(super.map(function), fileType);
    }

    @Override
    public TransformableRDD filter(Function function) {
        return new TransformableRDD(super.filter(function), fileType);
    }

    @Override
    public TransformableRDD cache() {
        return new TransformableRDD(super.cache(), fileType);
    }

    @Override
    public TransformableRDD coalesce(int numPartitions) {
        return new TransformableRDD(super.coalesce(numPartitions), fileType);
    }

    @Override
    public TransformableRDD coalesce(int numPartitions, boolean shuffle) {
        return new TransformableRDD(super.coalesce(numPartitions, shuffle), fileType);
    }

    @Override
    public TransformableRDD distinct() {
        return new TransformableRDD(super.distinct(), fileType);
    }

    @Override
    public TransformableRDD distinct(int numPartition) {
        return new TransformableRDD(super.distinct(numPartition), fileType);
    }

    @Override
    public TransformableRDD flatMap(FlatMapFunction flatmapFunction) {
        return new TransformableRDD(super.flatMap(flatmapFunction), fileType);
    }

    @Override
    public TransformableRDD intersection(JavaRDD other) {
        return new TransformableRDD(super.intersection(other), fileType);
    }

    public TransformableRDD intersection(TransformableRDD other) {
        return new TransformableRDD(super.intersection(other), fileType);
    }

    @Override
    public TransformableRDD persist(StorageLevel newLevel) {
        return new TransformableRDD(super.persist(newLevel), fileType);
    }

    @Override
    public TransformableRDD unpersist() {
        return new TransformableRDD(super.unpersist(), fileType);
    }

    @Override
    public TransformableRDD unpersist(boolean blocking) {
        return new TransformableRDD(super.unpersist(blocking), fileType);
    }

    @Override
    public TransformableRDD union(JavaRDD other) {
        return new TransformableRDD(super.union(other), fileType);
    }

    public TransformableRDD union(TransformableRDD other) {
        return new TransformableRDD(super.union(other), fileType);
    }

    @Override
    public TransformableRDD mapPartitions(FlatMapFunction flatMapFunction) {
        return new TransformableRDD(super.mapPartitions(flatMapFunction), fileType);
    }

    @Override
    public TransformableRDD mapPartitions(FlatMapFunction flatMapFunction, boolean preservesPartitioning) {
        return new TransformableRDD(super.mapPartitions(flatMapFunction, preservesPartitioning), fileType);
    }

    @Override
    public TransformableRDD mapPartitionsWithIndex(Function2 function, boolean preservesPartitioning) {
        return new TransformableRDD(super.mapPartitionsWithIndex(function, preservesPartitioning), fileType);
    }

    @Override
    public TransformableRDD sortBy(Function function, boolean ascending, int numPartitions) {
        return new TransformableRDD(super.sortBy(function, ascending, numPartitions), fileType);
    }

    @Override
    public TransformableRDD setName(String name) {
        return new TransformableRDD(super.setName(name), fileType);
    }

    @Override
    public TransformableRDD subtract(JavaRDD other) {
        return new TransformableRDD(super.subtract(other), fileType);
    }

    @Override
    public TransformableRDD subtract(JavaRDD other, int numPartitions) {
        return new TransformableRDD(super.subtract(other, numPartitions), fileType);
    }

    @Override
    public TransformableRDD subtract(JavaRDD other, Partitioner partitioner) {
        return new TransformableRDD(super.subtract(other, partitioner), fileType);
    }

    public TransformableRDD sample(boolean withReplacement, double fraction) {
        return new TransformableRDD(super.sample(withReplacement, fraction), fileType);
    }

    public TransformableRDD sample(boolean withReplacement, double fraction, long seed) {
        return new TransformableRDD(super.sample(withReplacement, fraction, seed), fileType);
    }

    @Override
    public TransformableRDD repartition(int numPartitions) {
        return new TransformableRDD(super.repartition(numPartitions), fileType);
    }

    @Override
    public TransformableRDD pipe(String command) {
        return new TransformableRDD(super.pipe(command), fileType);
    }

    @Override
    public TransformableRDD pipe(List<String> command) {
        return new TransformableRDD(super.pipe(command), fileType);
    }

    @Override
    public TransformableRDD pipe(List<String> command, Map<String, String> env) {
        return new TransformableRDD(super.pipe(command, env), fileType);
    }

    @Override
    public TransformableRDD wrapRDD(RDD rdd) {
        return new TransformableRDD(super.wrapRDD(rdd), fileType);
    }
}
