package org.apache.prepbuddy.cleansers.dedupe;

import org.apache.prepbuddy.qualityanalyzers.FileType;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

public class DuplicationHandler implements Serializable {

    public static JavaRDD<String> deduplicateByColumns(JavaRDD<String> inputRecords, final List<Integer> columnIndexes, final FileType fileType) {
        final JavaPairRDD<Long, String> fingerprintedRecords = inputRecords.mapToPair(new PairFunction<String, Long, String>() {
            @Override
            public Tuple2<Long, String> call(String record) throws Exception {
                long fingerprint = generateFingerprint(record, columnIndexes, fileType);
                return new Tuple2<>(fingerprint, record);
            }
        });

        JavaPairRDD<Long, String> uniqueRecordsWithKeys = fingerprintedRecords.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String accumulator, String fullRecord) throws Exception {
                return fullRecord;
            }
        });
        return uniqueRecordsWithKeys.values();
    }

    public static JavaRDD<String> deduplicate(JavaRDD<String> inputRDD) {
        return deduplicateByColumns(inputRDD, null, null);
    }


    public static JavaRDD<String> detectDuplicatesByColumns(JavaRDD<String> inputRecords, final List<Integer> columnIndexes, final FileType fileType) {
        final JavaPairRDD<Long, Tuple2<String, Integer>> fingerprintedRDD = inputRecords.mapToPair(new PairFunction<String, Long, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<Long, Tuple2<String, Integer>> call(String record) throws Exception {
                long fingerprint = generateFingerprint(record, columnIndexes, fileType);
                Tuple2<String, Integer> recordOnePair = new Tuple2<>(record, 1);

                return new Tuple2<>(fingerprint, recordOnePair);
            }
        });

        return getDuplicatesOnly(fingerprintedRDD);
    }

    public static JavaRDD<String> detectDuplicates(JavaRDD<String> inputRecords) {
        return detectDuplicatesByColumns(inputRecords, null, null);
    }

    public static JavaRDD<String> duplicatesAt(TransformableRDD inputRecords, final int columnIndex, final FileType fileType) {
        final JavaPairRDD<Long, Tuple2<String, Integer>> fingerprintedRDD = inputRecords.mapToPair(new PairFunction<String, Long, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<Long, Tuple2<String, Integer>> call(String record) throws Exception {
                long fingerprint = generateFingerprint(record, Arrays.asList(columnIndex), fileType);
                Tuple2<String, Integer> recordOnePair = new Tuple2<>(fileType.parseRecord(record)[columnIndex], 1);
                return new Tuple2<>(fingerprint, recordOnePair);
            }
        });

        return getDuplicatesOnly(fingerprintedRDD);
    }

    private static JavaRDD<String> getDuplicatesOnly(JavaPairRDD<Long, Tuple2<String, Integer>> fingerprintedRDD) {
        final JavaPairRDD<Long, Tuple2<String, Integer>> fingerprintedRecordCount = fingerprintedRDD.reduceByKey(new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> accumulator, Tuple2<String, Integer> currentRecordOnePair) throws Exception {
                int totalRecordOccurrence = accumulator._2() + currentRecordOnePair._2();
                return new Tuple2<>(accumulator._1(), totalRecordOccurrence);
            }
        });

        JavaPairRDD<Long, Tuple2<String, Integer>> duplicateRecords = fingerprintedRecordCount.filter(new Function<Tuple2<Long, Tuple2<String, Integer>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Long, Tuple2<String, Integer>> fingerprintRecordPair) throws Exception {
                Tuple2<String, Integer> recordWithOccurrence = fingerprintRecordPair._2();
                Integer numberOfOccurrence = recordWithOccurrence._2();
                return numberOfOccurrence != 1;
            }
        });
        return duplicateRecords.values().map(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> recordCountPair) throws Exception {
                return recordCountPair._1();
            }
        });
    }

    private static long generateFingerprint(String record, List<Integer> columnIndexes, FileType fileType) {
        if (columnIndexes != null) {
            String[] recordAsArray = fileType.parseRecord(record);
            record = "";
            for (Integer columnIndex : columnIndexes) {
                record += recordAsArray[columnIndex];
            }
        }

        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
            md.update(record.toUpperCase().getBytes(), 0, record.length());
            return new BigInteger(1, md.digest()).longValue();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
