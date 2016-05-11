package org.apache.prepbuddy.datacleansers;

import org.apache.prepbuddy.coreops.RowTransformation;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Deduplication implements Serializable, RowTransformation {

    public JavaRDD apply(JavaRDD inputRecords, FileType type) {
        final JavaPairRDD fingerprintedRecords = inputRecords.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String record) throws Exception {
                String fingerprint = generateFingerprint(record.toLowerCase());
                return new Tuple2<String, String>(fingerprint, record);
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

    private String generateFingerprint(String record) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
            md.update(record.getBytes());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return convertToHexString(md.digest());
    }

    private String convertToHexString(byte[] byteData) {
        StringBuffer hexString = new StringBuffer();
        for (byte byteValue : byteData) {
            String hex = Integer.toHexString(0xff & byteValue);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
