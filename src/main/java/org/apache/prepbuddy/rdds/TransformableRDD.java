package org.apache.prepbuddy.rdds;

import com.n1analytics.paillier.PaillierContext;
import com.n1analytics.paillier.PaillierPublicKey;
import org.apache.prepbuddy.datacleansers.Deduplication;
import org.apache.prepbuddy.datacleansers.MissingDataHandler;
import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.encryptors.HomomorphicallyEncryptedRDD;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.prepbuddy.groupingops.ClusteringAlgorithm;
import org.apache.prepbuddy.groupingops.Clusters;
import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.prepbuddy.transformation.ColumnMerger;
import org.apache.prepbuddy.transformation.ColumnSplitter;
import org.apache.prepbuddy.transformation.MarkerPredicate;
import org.apache.prepbuddy.utils.EncryptionKeyPair;
import org.apache.prepbuddy.utils.Replacement;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class TransformableRDD extends JavaRDD<String> {
    private FileType fileType;

    public TransformableRDD(JavaRDD rdd, FileType fileType) {
        super(rdd.rdd(), rdd.rdd().elementClassTag());
        this.fileType = fileType;
    }

    public TransformableRDD(JavaRDD rdd) {
        this(rdd, FileType.CSV);
    }

    public HomomorphicallyEncryptedRDD encryptHomomorphically(final EncryptionKeyPair keyPair, final int columnIndex) {
        final PaillierPublicKey publicKey = keyPair.getPublicKey();
        final PaillierContext signedContext = publicKey.createSignedContext();
        JavaRDD map = wrapRDD(rdd()).map(new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                String[] values = fileType.parseRecord(row);
                String numericValue = values[columnIndex];
                values[columnIndex] = signedContext.encrypt(Double.parseDouble(numericValue)).toString();
                return fileType.join(values);
            }
        });
        return new HomomorphicallyEncryptedRDD(map.rdd(), keyPair, fileType);
    }


    public TransformableRDD deduplicate() {
        JavaRDD transformed = new Deduplication().apply(this);
        return new TransformableRDD(transformed, fileType);
    }

    public TransformableRDD removeRows(RowPurger.Predicate predicate) {
        JavaRDD<String> transformed = new RowPurger(predicate).apply(this, fileType);
        return new TransformableRDD(transformed, fileType);
    }

    public TransformableRDD impute(final int columnIndex, final MissingDataHandler handler) {
        JavaRDD<String> transformed = this.map(new Function<String, String>() {

            @Override
            public String call(String record) throws Exception {
                String[] recordAsArray = fileType.parseRecord(record);
                String value = recordAsArray[columnIndex];
                String replacementValue = value;
                if (value == null || value.trim().isEmpty()) {
                    replacementValue = handler.handleMissingData(new RowRecord(recordAsArray));
                }
                recordAsArray[columnIndex] = replacementValue;
                return fileType.join(recordAsArray);
            }
        });
        return new TransformableRDD(transformed, fileType);
    }

    public TransformableRDD replace(final int columnIndex, final Replacement... replacements) {
        JavaRDD<String> transformed = this.map(new Function<String, String>() {

            @Override
            public String call(String record) throws Exception {
                String[] recordAsArray = fileType.parseRecord(record);
                for (Replacement replacement : replacements) {
                    recordAsArray[columnIndex] = replacement.replace(recordAsArray[columnIndex]);
                }
                return fileType.join(recordAsArray);
            }
        });
        return new TransformableRDD(transformed, fileType);
    }

    public TextFacets listFacets(final int columnIndex) {
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

    public Clusters clusters(int columnIndex, ClusteringAlgorithm algorithm) {
        TextFacets textFacets = this.listFacets(columnIndex);
        JavaPairRDD<String, Integer> rdd = textFacets.rdd();
        List<Tuple2<String, Integer>> tuples = rdd.collect();

        return algorithm.getClusters(tuples);
    }

    public TransformableRDD splitColumn(final int columnIndex, final ColumnSplitter columnSplitter) {
        JavaRDD<String> transformed = this.map(new Function<String, String>() {
            @Override
            public String call(String record) throws Exception {
                String[] recordAsArray = fileType.parseRecord(record);
                String[] transformedRow = columnSplitter.apply(recordAsArray, columnIndex);
                return fileType.join(transformedRow);
            }
        });
        return new TransformableRDD(transformed, fileType);
    }

    public TransformableRDD mergeColumns(final ColumnMerger columnMerger) {
        JavaRDD<String> transformed = this.map(new Function<String, String>() {
            @Override
            public String call(String record) throws Exception {
                String[] recordAsArray = fileType.parseRecord(record);
                String[] transformedRow = columnMerger.apply(recordAsArray);
                return fileType.join(transformedRow);
            }
        });
        return new TransformableRDD(transformed, fileType);
    }

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

    public TransformableRDD mapByFlag(final String flag, final int symbolColumnIndex, final Function<String, String> mapFunction) {
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
}
