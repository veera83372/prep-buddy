package org.apache.prepbuddy.datacleansers;

import org.apache.prepbuddy.coreops.RowTransformation;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.JavaSerializer;
import scala.Tuple2;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Deduplication extends JavaSerializer implements RowTransformation {

<<<<<<< HEAD
    public JavaRDD apply(JavaRDD inputRecords, FileType type) {
        final JavaPairRDD fingerprintedRecords = inputRecords.mapToPair(new PairFunction<String, String, String>() {
=======
    public JavaRDD<String> apply(JavaRDD inputRecords) {
        JavaPairRDD fingerprintedRecords = inputRecords.mapToPair(new PairFunction<String, Long, String>() {
>>>>>>> 110da91c6309bd05aec2ad62bfd9e1005bbb5911
            @Override
            public Tuple2<Long, String> call(String record) throws Exception {
                long fingerprint = generateFingerprint(record.toLowerCase());
                return new Tuple2<>(fingerprint, record);
            }
        });

        JavaPairRDD uniqueRecordsWithKeys = fingerprintedRecords.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String accumulator, String fullRecord) throws Exception {
                return fullRecord;
            }
        });
        return uniqueRecordsWithKeys.values();
    }

    private long generateFingerprint(String record) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
            md.update(record.getBytes(), 0, record.length());
            return new BigInteger(1, md.digest()).longValue();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
