package org.apache.prepbuddy.rdds;

import com.n1analytics.paillier.PaillierContext;
import com.n1analytics.paillier.PaillierPublicKey;
import org.apache.commons.lang.StringUtils;
import org.apache.prepbuddy.datacleansers.Deduplication;
import org.apache.prepbuddy.datacleansers.MissingDataHandler;
import org.apache.prepbuddy.datacleansers.ReplacementFunction;
import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.encryptors.HomomorphicallyEncryptedRDD;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.prepbuddy.groupingops.Clusters;
import org.apache.prepbuddy.groupingops.FingerprintingAlgorithms;
import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.prepbuddy.utils.EncryptionKeyPair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.apache.prepbuddy.groupingops.FingerprintingAlgorithms.generateSimpleFingerprint;

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
        JavaRDD<String> transformed = new Deduplication().apply(this);
        return new TransformableRDD(transformed, fileType);
    }

    public TransformableRDD removeRows(RowPurger.Predicate predicate) {
        JavaRDD<String> transformed = new RowPurger(predicate).apply(this);
        return new TransformableRDD(transformed, fileType);
    }

    public TransformableRDD impute(int columnIndex, MissingDataHandler handler) {
        JavaRDD<String> transformed = this.map(new Function<String, String>() {

            @Override
            public String call(String record) throws Exception {
                String[] recordAsArray = fileType.parseRecord(record);
                String value = recordAsArray[columnIndex];
                String replacementValue = value;
                if (value == null || value.trim().isEmpty()) {
                    replacementValue = handler.handleMissingData(recordAsArray);
                }
                recordAsArray[columnIndex] = replacementValue;
                return fileType.join(recordAsArray);
            }
        });
        return new TransformableRDD(transformed, fileType);
    }

    public TransformableRDD replace(int columnIndex, ReplacementFunction function) {
        JavaRDD<String> transformed = this.map(new Function<String, String>() {

            @Override
            public String call(String record) throws Exception {
                String[] recordAsArray = fileType.parseRecord(record);
                recordAsArray[columnIndex] = function.replace(recordAsArray[columnIndex]);
                return fileType.join(recordAsArray);
            }
        });
        return new TransformableRDD(transformed, fileType);
    }

    public TextFacets listFacets(int columnIndex) {
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

    public Clusters clusterUsingSimpleFingerprint(int columnIndex) {
        Clusters clusters = new Clusters();
        TextFacets textFacets = this.listFacets(columnIndex);
        JavaPairRDD<String, Integer> rdd = textFacets.rdd();
        List<Tuple2<String, Integer>> tuples = rdd.collect();

        for (Tuple2<String, Integer> tuple : tuples) {
            String key = generateSimpleFingerprint(tuple._1());
            clusters.add(key, tuple);
        }
        return clusters;
    }

    public Clusters clusterUsingNGramFingerprint(int columnIndex, int nGram) {
        Clusters clusters = new Clusters();
        TextFacets textFacets = this.listFacets(columnIndex);
        JavaPairRDD<String, Integer> rdd = textFacets.rdd();
        List<Tuple2<String, Integer>> tuples = rdd.take((int) rdd.count());

        for (Tuple2<String, Integer> tuple : tuples) {
            String key = FingerprintingAlgorithms.generateNGramFingerprint(tuple._1(), nGram);
            clusters.add(key, tuple);
        }
        return clusters;
    }

    public Clusters clusterUsingLevenshteinDistance(int columnIndex) {
        Clusters clusters = new Clusters();
        TextFacets textFacets = this.listFacets(columnIndex);
        JavaPairRDD<String, Integer> rdd = textFacets.rdd();
        List<Tuple2<String, Integer>> tuples = rdd.take((int) rdd.count());

        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < tuples.size(); i++) {
            Tuple2<String, Integer> tuple = tuples.get(i);
            String tupleKey = tuple._1();
            clusters.add(tupleKey, tuple);
            for (int j = i + 1; j < tuples.size(); j++) {
                Tuple2<String, Integer> otherTuple = tuples.get(j);
                String otherTupleKey = otherTuple._1();
                int distance = StringUtils.getLevenshteinDistance(tupleKey, otherTupleKey);
                if (distance < 4 && !(indexes.contains(j))) {
                    clusters.add(tupleKey, otherTuple);
                    indexes.add(j);
                }
            }
        }
        return clusters;
    }
}
