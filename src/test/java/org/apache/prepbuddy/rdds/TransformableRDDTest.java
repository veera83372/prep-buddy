package org.apache.prepbuddy.rdds;

import org.apache.commons.io.FileUtils;
import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.cleansers.imputation.ImputationStrategy;
import org.apache.prepbuddy.cluster.Cluster;
import org.apache.prepbuddy.cluster.Clusters;
import org.apache.prepbuddy.cluster.SimpleFingerprintAlgorithm;
import org.apache.prepbuddy.encryptors.HomomorphicallyEncryptedRDD;
import org.apache.prepbuddy.qualityanalyzers.FileType;
import org.apache.prepbuddy.smoothers.SimpleMovingAverageMethod;
import org.apache.prepbuddy.smoothers.WeightedMovingAverageMethod;
import org.apache.prepbuddy.smoothers.Weights;
import org.apache.prepbuddy.utils.EncryptionKeyPair;
import org.apache.prepbuddy.utils.PivotTable;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransformableRDDTest extends SparkTestCase {

    @Test
    public void shouldEncryptAColumn() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("1,X", "2,Y", "3,Z", "4,A"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset, FileType.CSV);
        EncryptionKeyPair keyPair = new EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair, 0);
        JavaRDD<String> dataSet = encryptedRDD.decrypt(0);
        assertEquals(dataSet.collect(), initialDataset.collect());
    }

    @Test
    public void shouldBeAbleToGetSumOfTheEncryptedColumn() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("5,d,", "2,43", "13,re", "4,42"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset, FileType.CSV);
        EncryptionKeyPair keyPair = new EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair, 0);
        BigInteger sum = encryptedRDD.sum(0);

        assertEquals(new BigInteger("24"), sum);
    }

    @Test
    public void shouldBeAbleToGetAverageOfTheEncryptedColumn() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,3,s", "1,2,Y", "Z,1,p", "A,1,N"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset, FileType.CSV);
        EncryptionKeyPair keyPair = new EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair, 1);
        double average = encryptedRDD.average(1);

        assertEquals(1.75, average, 0.01);
    }

    @Test
    public void shouldBeAbleToGetSumOfTheEncryptedColumnForDouble() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("3.1,X", "2.1,Y", "13.1,Z", "4.1,A"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset, FileType.CSV);
        EncryptionKeyPair keyPair = new EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair, 0);
        BigInteger sum = encryptedRDD.sum(0);

        assertEquals(new BigInteger("22"), sum);
    }

    @Test
    public void shouldBeAbleToReadAndWrite() throws IOException {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("3,X", "2,Y", "13,Z", "4,A"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset, FileType.CSV);
        EncryptionKeyPair keyPair = new EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair, 0);

        String path = "data/EncryptedData";
        FileUtils.deleteDirectory(new File(path));
        encryptedRDD.saveAsTextFile(path);
        JavaRDD<String> javaRDD = javaSparkContext.textFile(path);
        HomomorphicallyEncryptedRDD rdd = new HomomorphicallyEncryptedRDD(javaRDD, keyPair, FileType.CSV);
        JavaRDD<String> decrypt = rdd.decrypt(0);

        assertEquals(decrypt.collect(), initialDataset.collect());
    }

    @Test
    public void shouldChangeValueOfFieldOfMatchesClusters() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("CLUSTER Of Finger print", "finger print of cluster", "finger print for cluster"));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        Clusters clusters = initialRDD.clusters(0, new SimpleFingerprintAlgorithm());

        List<Cluster> clustersWithSizeGreaterThanOne = clusters.getClustersWithSizeGreaterThan(1);
        TransformableRDD afterMergeCluster = initialRDD.replaceValues(clustersWithSizeGreaterThanOne.get(0), "Finger print", 0);

        List<String> listOfValues = afterMergeCluster.collect();

        assertTrue(listOfValues.contains("Finger print"));
    }

    @Test
    public void shouldBeAbleToDeduplicateRecordsBasedWholeRecord() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        TransformableRDD deduplicatedRDD = initialRDD.deduplicate();
        assertEquals(4, deduplicatedRDD.count());
    }

    @Test
    public void shouldBeAbleToDeduplicateRecordsBasedOnColumns() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        TransformableRDD deduplicatedRDD = initialRDD.deduplicate(Arrays.asList(0, 3));
        assertEquals(3, deduplicatedRDD.count());
    }

    @Test
    public void shouldBeAbleToDetectDeduplicateRecordsBasedOnColumns() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        TransformableRDD duplicates = initialRDD.getDuplicates(Arrays.asList(0, 3));
        assertEquals(1, duplicates.count());
    }

    @Test
    public void shouldBeAbleToDetectDeduplicateRecordsBasedOnWholeRecord() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        TransformableRDD duplicates = initialRDD.getDuplicates();
        assertEquals(0, duplicates.count());
    }

    @Test
    public void shouldBeAbleToSelectAColumnFromTheDataSet() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        JavaRDD<String> columnValues = initialRDD.select(0);
        List<String> expectedValues = Arrays.asList("Smith", "John", "John", "Smith");
        assertEquals(expectedValues, columnValues.collect());
    }

    @Test
    public void sizeShouldGiveTheNumberOfColumnInRdd() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        int size = initialRDD.getNumberOfColumns();
        assertEquals(4, size);
    }

    @Test
    public void shouldBeAbleToSelectSomeFeaturesFromTheDataSet() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);

        TransformableRDD selectedFeatures = initialRDD.select(0, 1);
        List<String> expected = Arrays.asList("Smith,Male", "John,Male", "John,Male", "Smith,Male");
        assertEquals(expected, selectedFeatures.collect());
    }

    @Test
    public void pivotByCountsShouldGiveCountsWithGivenColumns() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "known,new,long,home,skips",
                "unknown,new,short,work,reads",
                "unknown,follow Up,long,work,skips",
                "known,follow Up,long,home,skips",
                "known,new,short,home,reads",
                "known,follow Up,long,work,skips",
                "unknown,follow Up,short,work,skips",
                "unknown,new,short,work,reads",
                "known,follow Up,long,home,skips",
                "known,new,long,work,skips",
                "unknown,follow Up,short,home,skips",
                "known,new,long,work,skips",
                "known,follow Up,short,home,reads",
                "known,new,short,work,reads",
                "known,new,short,home,reads",
                "known,follow Up,short,work,reads",
                "known,new,short,home,reads",
                "unknown,new,short,work,reads"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataSet);
        PivotTable<Integer> pivotTable = initialRDD.pivotByCount(4, new int[]{0, 1, 2, 3});
        int valueAtSkipsLong = pivotTable.valueAt("skips", "long");
        assertEquals(valueAtSkipsLong, 7);

        int valueAtReadsLong = pivotTable.valueAt("reads", "long");
        assertEquals(valueAtReadsLong, 0);
    }

    @Test
    public void map_reduce_andOtherJavaRDDFunctionsShouldBeAbleToReturnTransformableRDD() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("52,32,53", "23,42,64"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset);

        TransformableRDD mappedResult = transformableRDD.map(new Function<String, String>() {
            @Override
            public String call(String record) throws Exception {
                return record + ",x";
            }
        });

        TransformableRDD filterResult = mappedResult.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String record) throws Exception {
                return !record.equals("23,42,64,x");
            }
        });

        assertEquals("52,32,53,x", filterResult.first());

    }

    @Test
    public void smoothShouldSmoothDataUsingSimpleMovingAverages() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "52,3,53", "23,4,64", "23,5,64", "23,6,64", "23,7,64", "23,8,64", "23,9,64"
        ), 3);
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset);
        JavaRDD<Double> transformed = transformableRDD.smooth(1, new SimpleMovingAverageMethod(3));

        Double excepted = 4.0;
        assertEquals(excepted, transformed.first());

        List<Double> expectedList = Arrays.asList(4.0, 5.0, 6.0, 7.0, 8.0);
        assertEquals(expectedList, transformed.collect());
    }

    @Test
    public void smoothShouldSmoothDataUsingWeightedMovingAverages() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "52,10,53", "23,12,64", "23,16,64", "23,13,64", "23,17,64", "23,19,64", "23,15,64"
        ), 3);
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset);

        Weights weights = new Weights(3);
        weights.add(0.166);
        weights.add(0.333);
        weights.add(0.5);
        JavaRDD<Double> transformed = transformableRDD.smooth(1, new WeightedMovingAverageMethod(3, weights));

        Double expected = 13.66;
        Double actual = Double.parseDouble(new DecimalFormat("##.##").format(transformed.first()));
        assertEquals(expected, actual);

    }

    @Test
    public void shouldImputeTheMissingValueByConsideringGivenHints() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "1,NULL,2,3,4",
                "2,N/A,23,21,23",
                "3,N/A,21,32,32",
                "4,-,2,3,4",
                "5,,54,32,54",
                "6,32,22,33,23"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataSet);

        List<String> imputedRDD = initialRDD.impute(1, new ImputationStrategy() {
            @Override
            public void prepareSubstitute(TransformableRDD rdd, int missingDataColumn) {

            }

            @Override
            public String handleMissingData(RowRecord record) {
                return "X";
            }
        }, Arrays.asList("N/A", "-", "NA", "NULL")).collect();

        assertTrue(imputedRDD.contains("1,X,2,3,4"));
        assertTrue(imputedRDD.contains("2,X,23,21,23"));
        assertTrue(imputedRDD.contains("3,X,21,32,32"));
        assertTrue(imputedRDD.contains("4,X,2,3,4"));
        assertTrue(imputedRDD.contains("5,X,54,32,54"));
        assertTrue(imputedRDD.contains("6,32,22,33,23"));
    }

    @Test
    public void shouldAddTheSpecifiedColumnsOfGivenTransformableRDDToTheCurrentTransformableRDD() {
        JavaRDD<String> initialSpelledNumbers = javaSparkContext.parallelize(Arrays.asList(
                "One,Two,Three",
                "Four,Five,Six",
                "Seven,Eight,Nine",
                "Ten,Eleven,Twelve"
        ));
        TransformableRDD spelledNumbers = new TransformableRDD(initialSpelledNumbers);
        JavaRDD<String> initialNumericData = javaSparkContext.parallelize(Arrays.asList(
                "1\t2\t3\tX",
                "4\t5\t6\tY",
                "7\t8\t9\tZ",
                "10\t11\t12\tA"
        ));
        TransformableRDD numericData = new TransformableRDD(initialNumericData, FileType.TSV);

        List<String> result = spelledNumbers.addColumns(Arrays.asList(0, 1, 2), numericData).collect();

        assertTrue(result.contains("One,Two,Three,1,2,3"));
        assertTrue(result.contains("Four,Five,Six,4,5,6"));
        assertTrue(result.contains("Seven,Eight,Nine,7,8,9"));
        assertTrue(result.contains("Ten,Eleven,Twelve,10,11,12"));
    }
}
