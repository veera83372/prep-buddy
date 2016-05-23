package org.apache.prepbuddy.rdds;

import com.n1analytics.paillier.PaillierContext;
import com.n1analytics.paillier.PaillierPublicKey;
import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.datacleansers.dedupe.DuplicationHandler;
import org.apache.prepbuddy.datacleansers.imputation.ImputationStrategy;
import org.apache.prepbuddy.encryptors.HomomorphicallyEncryptedRDD;
import org.apache.prepbuddy.exceptions.ApplicationException;
import org.apache.prepbuddy.exceptions.ErrorMessages;
import org.apache.prepbuddy.groupingops.Cluster;
import org.apache.prepbuddy.groupingops.ClusteringAlgorithm;
import org.apache.prepbuddy.groupingops.Clusters;
import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.prepbuddy.transformations.MarkerPredicate;
import org.apache.prepbuddy.transformations.MergePlan;
import org.apache.prepbuddy.transformations.SplitPlan;
import org.apache.prepbuddy.typesystem.DataType;
import org.apache.prepbuddy.typesystem.FileType;
import org.apache.prepbuddy.typesystem.TypeAnalyzer;
import org.apache.prepbuddy.utils.EncryptionKeyPair;
import org.apache.prepbuddy.utils.Replacement;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;

public class TransformableRDD extends JavaRDD<String> {
    public FileType fileType;

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
        JavaRDD map = this.map(new Function<String, String>() {
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
        JavaRDD transformed = new DuplicationHandler().deduplicate(this);
        return new TransformableRDD(transformed, fileType);
    }

    public TransformableRDD detectDuplicates() {
        JavaRDD transformed = new DuplicationHandler().detectDuplicates(this);
        return new TransformableRDD(transformed);
    }

    public TransformableRDD removeRows(RowPurger.Predicate predicate) {
        JavaRDD<String> transformed = new RowPurger(predicate).apply(this, fileType);
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

    public DataType inferType(final int columnIndex) {
        List<String> rowSamples = this.takeSample(false, 100);
        List<String> columnSamples = new LinkedList<>();
        for (String row : rowSamples) {
            String[] strings = fileType.parseRecord(row);
            columnSamples.add(strings[columnIndex]);
        }
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(columnSamples);
        return typeAnalyzer.getType();
    }

    public TransformableRDD dropFlag(final int symbolColumnIndex) {
        return this.removeColumn(symbolColumnIndex);
    }

    public TransformableRDD removeColumn(final int columnIndex) {
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

    public TransformableRDD replaceValues(final Cluster cluster, final String newValue, final int columnIndex) {
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

    public TransformableRDD impute(final int columnIndex, final ImputationStrategy strategy) {
        strategy.prepareSubstitute(this, columnIndex);
        JavaRDD<String> transformed = this.map(new Function<String, String>() {

            @Override
            public String call(String record) throws Exception {
                String[] recordAsArray = fileType.parseRecord(record);
                String value = recordAsArray[columnIndex];
                String replacementValue = value;
                if (value == null || value.trim().isEmpty()) {
                    replacementValue = strategy.handleMissingData(new RowRecord(recordAsArray));
                }
                recordAsArray[columnIndex] = replacementValue;
                return fileType.join(recordAsArray);
            }
        });
        return new TransformableRDD(transformed, fileType);
    }


    public JavaDoubleRDD toDoubleRDD(final int columnIndex) {
        DataType dataType = inferType(columnIndex);
        boolean isNumeric = DataType.INTEGER.equals(dataType) || DataType.DECIMAL.equals(dataType);
        if (!isNumeric)
            throw new ApplicationException(ErrorMessages.COLUMN_VALUES_ARE_NOT_NUMERIC);

        return this.mapToDouble(new DoubleFunction<String>() {
            @Override
            public double call(String row) throws Exception {
                String[] recordAsArray = fileType.parseRecord(row);
                String columnValue = recordAsArray[columnIndex];
                if (!columnValue.trim().isEmpty())
                    return Double.parseDouble(recordAsArray[columnIndex]);
                return 0;
            }
        });
    }

    public JavaDoubleRDD toMultipliedRdd(final int columnIndex, final int xColumnIndex) {
        return this.mapToDouble(new DoubleFunction<String>() {
            @Override
            public double call(String row) throws Exception {
                String[] recordAsArray = fileType.parseRecord(row);
                String columnValue = recordAsArray[columnIndex];
                String otherColumnValue = recordAsArray[xColumnIndex];
                System.out.println("otherColumnValue = " + otherColumnValue);
                System.out.println("columnValue = " + columnValue);
                if (columnValue.trim().isEmpty() || otherColumnValue.trim().isEmpty())
                    return 0;
                return Double.parseDouble(recordAsArray[columnIndex]) * Double.parseDouble(recordAsArray[xColumnIndex]);

            }
        });
    }
}
