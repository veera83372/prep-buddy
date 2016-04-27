package org.apache.prepbuddy.transformations.deduplication;

import org.apache.prepbuddy.transformations.DataTransformation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DeduplicationTransformation implements DataTransformation, Serializable {

    @Override
    public JavaRDD apply(JavaRDD inputRecords) throws NoSuchAlgorithmException {
        final JavaPairRDD hashedRecordPair = inputRecords.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String record) throws Exception {
                String md5Hash = generateMD5String(record.toLowerCase());
                return new Tuple2<String, String>(md5Hash, record);
            }
        });

        JavaPairRDD hashedUniqueRecords = hashedRecordPair.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String accumulator, String currentValue) throws Exception {
                return currentValue;
            }
        });

        JavaRDD uniqueRecords = hashedUniqueRecords.map(new Function<Tuple2<String, String>,String>() {
            @Override
            public String call(Tuple2<String,String> record) throws Exception {
                return record._2();
            }
        });
        return uniqueRecords;
    }

    private String generateMD5String(String record) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(record.getBytes());
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
